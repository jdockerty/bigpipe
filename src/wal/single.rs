use std::io::{self, BufReader, Read};
use std::path::Path;
use std::{fs::File, io::Write, path::PathBuf};

use bytes::{BufMut, Bytes, BytesMut};
use hashbrown::HashMap;
use prost::Message;
use tracing::{debug, error, info, warn};
use walkdir::{DirEntry, WalkDir};

use super::DEFAULT_MAX_SEGMENT_SIZE;
use super::MAX_SEGMENT_BUFFER_SIZE;
use super::WAL_DEFAULT_ID;
use super::WAL_EXTENSION;
use crate::data_types::wal_proto::SegmentEntry as SegmentEntryProto;
use crate::data_types::{namespace::Namespace, value::BigPipeValue, wal::WalOperation};
use crate::ServerMessage;

/// A write-ahead log (WAL) implementation.
///
/// The WAL is used to write bigpipe operations
/// into an append-only file for durability, before
/// becoming available to consumers.
#[derive(Debug)]
pub struct Wal {
    active_segment: Segment,
    segment_max_size: usize,
    closed_segments: Vec<u64>,
    directory: PathBuf,
}

fn find_segment_ids(path: impl AsRef<Path>) -> Vec<u64> {
    let dir = walkdir::WalkDir::new(&path)
        .max_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().to_string_lossy().contains(WAL_EXTENSION));

    let mut segment_ids: Vec<u64> = dir
        .into_iter()
        .map(|entry| parse_segment_id(&entry))
        .collect();
    segment_ids.sort();
    println!("{segment_ids:?}");
    segment_ids
}

impl Wal {
    pub fn try_new(
        directory: PathBuf,
        segment_max_size: Option<usize>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let closed_segments = find_segment_ids(&directory);

        // Segments are sorted, the last element is the largest current segment.
        let id = closed_segments
            .last()
            .and_then(|s| Some(s + 1))
            .unwrap_or(WAL_DEFAULT_ID);

        Ok(Self {
            directory: directory.clone(),
            segment_max_size: segment_max_size.unwrap_or(DEFAULT_MAX_SEGMENT_SIZE),
            active_segment: Segment::try_new(id, directory.join(format!("{id}{WAL_EXTENSION}")))?,
            closed_segments,
        })
    }

    /// Replay an individual [`Wal`], returning the inner data representation.
    /// This requires reading ALL segments that are available.
    pub fn replay<P: AsRef<Path>>(directory: P) -> Option<(u64, HashMap<Namespace, BigPipeValue>)> {
        let mut highest_segment_id: u64 = WAL_DEFAULT_ID;
        let mut segment_paths = Vec::new();

        for entry in WalkDir::new(directory)
            .max_depth(1) // current directory only
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().to_string_lossy().contains(WAL_EXTENSION))
        {
            let segment_id = parse_segment_id(&entry);
            if segment_id >= highest_segment_id {
                highest_segment_id = segment_id;
            }
            segment_paths.push((segment_id, entry.path().to_path_buf()));
        }

        if segment_paths.is_empty() {
            return None;
        }

        // Ensure that the paths are replayed in the order they were
        // written.
        segment_paths.sort_by_key(|(id, _)| *id);

        let mut messages = HashMap::with_capacity(1000);
        for (_, path) in segment_paths {
            let segment = std::fs::File::open(&path).unwrap();
            let mut reader = BufReader::new(segment);

            while let Some(len) = read_varint(&mut reader).unwrap() {
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf).unwrap();

                let mut bytes = Bytes::from(buf);
                match SegmentEntryProto::decode(&mut bytes) {
                    Ok(entry) => {
                        let message_entry = entry.message_entry.expect("message is encoded");
                        // TODO: maps should be for offset id => physical offset in a segment now.
                        messages
                            .entry(Namespace::new(&message_entry.key))
                            .and_modify(|occupied_messages: &mut BigPipeValue| {
                                let message_entry = message_entry.clone();
                                occupied_messages.push(ServerMessage::new(
                                    message_entry.key,
                                    message_entry.value.into(),
                                    message_entry.timestamp,
                                ))
                            })
                            .or_insert_with(|| {
                                let mut messages = BigPipeValue::new();
                                messages.push(ServerMessage::new(
                                    message_entry.key,
                                    message_entry.value.into(),
                                    message_entry.timestamp,
                                ));
                                messages
                            });
                    }
                    Err(e) => {
                        error!(?e, "wal decoding error");
                        break;
                    }
                }
            }
        }

        Some((highest_segment_id, messages))
    }

    /// Write a message into the write-ahead log.
    pub fn write(
        &mut self,
        op: WalOperation,
    ) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        if self.active_segment.current_size >= self.segment_max_size {
            self.rotate()?;
        }
        let (sz, offset) = self.active_segment.write(op)?;
        self.messages += 1;
        Ok((sz, offset))
    }

    #[allow(dead_code)]
    /// Force flush the current active segment.
    pub(crate) fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.active_segment.flush()
    }

    /// Rotate the currently active WAL segment.
    pub(crate) fn rotate(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Force a flush to ensure that no data is lost before rotation.
        self.active_segment.flush()?;

        let current_id = self.active_segment.id();
        let next_id = current_id + 1;
        let next_path = self.directory.join(format!("{}{WAL_EXTENSION}", next_id));
        let (last_id, new_segment) = self.active_segment.next_segment(next_path.clone());
        self.active_segment = new_segment;

        info!(
            current_id,
            next_id,
            next_path = %next_path.to_string_lossy(),
            current_size = self.active_segment.current_size,
            max_size = self.segment_max_size,
            "rotating wal segment"
        );

        // Push the previously held 'current' only once the new segment is opened
        self.closed_segments.push(last_id.0);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn active_segment_path(&self) -> &Path {
        self.active_segment.path()
    }

    #[allow(dead_code)]
    pub fn closed_segments(&self) -> &[u64] {
        &self.closed_segments
    }
}

#[derive(Debug, Clone)]
struct SegmentId(u64);

#[derive(Debug)]
struct Segment {
    id: SegmentId,
    filepath: PathBuf,
    file: File,
    current_size: usize,
    buf: BytesMut,
}

impl Segment {
    /// Construct a new [`Segment`].
    fn try_new(id: u64, segment_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let buf = BytesMut::with_capacity(MAX_SEGMENT_BUFFER_SIZE as usize);
        Ok(Self {
            id: SegmentId(id),
            filepath: segment_path.clone(),
            file: File::create_new(segment_path)?,
            current_size: 0,
            buf,
        })
    }

    /// Perform a write into the [`Segment`], returning the number of bytes written.
    fn write(&mut self, op: WalOperation) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        let wal_operation = SegmentEntryProto::try_from(op)?.encode_length_delimited_to_vec();
        let size = wal_operation.len();

        self.current_size += size;
        self.buf.put_slice(&wal_operation);

        if self.buf.len() >= MAX_SEGMENT_BUFFER_SIZE as usize {
            self.flush()?;
        }

        Ok((size, self.current_size))
    }

    fn id(&self) -> u64 {
        self.id.0
    }

    /// Produce a new [`Segment`] and return the [`SegmentId`] of the current
    /// segment.
    fn next_segment(&self, segment_path: PathBuf) -> (SegmentId, Self) {
        (
            self.id.clone(),
            Self {
                id: SegmentId(self.id() + 1),
                current_size: 0,
                buf: BytesMut::with_capacity(MAX_SEGMENT_BUFFER_SIZE as usize),
                filepath: segment_path.clone(),
                file: File::create_new(segment_path).unwrap(),
            },
        )
    }

    /// Flush data for the [`Segment`] from the internal buffer to the underlying
    /// file.
    fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("flushing internal segment buffer");
        self.file.write_all(&self.buf)?;
        self.file.sync_all()?;
        self.buf.clear();
        Ok(())
    }

    /// Get the path to the currently active segment file.
    fn path(&self) -> &Path {
        &self.filepath
    }
}

// Adapted from <https://docs.rs/wyre/latest/src/wyre/lib.rs.html#53-70>
fn read_varint<R: Read>(reader: &mut R) -> io::Result<Option<usize>> {
    let mut buf = [0u8; 1];
    let mut shift = 0;
    let mut result: usize = 0;

    for _ in 0..10 {
        if reader.read(&mut buf)? == 0 {
            return Ok(None); // EOF
        }
        let byte = buf[0];
        result |= ((byte & 0x7F) as usize) << shift;
        if byte & 0x80 == 0 {
            return Ok(Some(result));
        }
        shift += 7;
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "varint too long",
    ))
}

fn parse_segment_id(entry: &DirEntry) -> u64 {
    debug!(?entry);
    entry
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .strip_suffix(WAL_EXTENSION)
        .expect("Only WAL files are returned by the filter")
        .parse::<u64>()
        .unwrap()
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn path_semantics() {
        let dir = TempDir::new().unwrap();

        let mut wal = Wal::try_new(dir.path().to_path_buf(), None).unwrap();

        let expected_path = dir.path().join(format!("{WAL_DEFAULT_ID}{WAL_EXTENSION}"));
        assert!(expected_path.exists());
        assert_eq!(wal.active_segment.path(), &expected_path);
        assert_eq!(wal.active_segment.id(), 0);

        wal.rotate().unwrap();
        let expected_path = dir.path().join(format!("1{WAL_EXTENSION}"));
        assert!(expected_path.exists());
        assert_eq!(wal.active_segment.path(), &expected_path);
        assert_eq!(wal.active_segment.id(), 1);
    }

    #[test]
    fn write() {
        let dir = TempDir::new().unwrap();

        let mut wal = Wal::try_new(dir.path().to_path_buf(), None).unwrap();

        wal.write(WalOperation::test_message(0)).unwrap();
        wal.flush().unwrap();

        assert!(wal.active_segment_path().exists());
        assert_ne!(
            wal.active_segment.file.metadata().unwrap().len(),
            0,
            "Flushing WAL expected a non-zero file size after writing"
        );
    }

    #[test]
    fn write_with_rotation() {
        let dir = TempDir::new().unwrap();

        let mut wal = Wal::try_new(dir.path().to_path_buf(), Some(24)).unwrap();

        // Known size of the write
        let server_msg_size = 17;

        for _ in 0..=2 {
            wal.write(WalOperation::test_message(0)).unwrap();
            wal.flush().unwrap();
        }

        assert_eq!(wal.active_segment.id(), 1, "Expected rotation");

        assert_eq!(
            wal.active_segment.file.metadata().unwrap().len(),
            server_msg_size,
            "A single write should be in the new file"
        );
        assert_eq!(
            std::fs::File::open(dir.path().join(format!("{WAL_DEFAULT_ID}{WAL_EXTENSION}")))
                .unwrap()
                .metadata()
                .unwrap()
                .len(),
            server_msg_size * 2,
            "Two writes are expected to the old segment"
        )
    }

    #[test]
    fn replay() {
        let dir = TempDir::new().unwrap();

        const TINY_SEGMENT_SIZE: usize = 64;
        let mut wal = Wal::try_new(dir.path().to_path_buf(), Some(TINY_SEGMENT_SIZE)).unwrap();

        for i in 0..100 {
            wal.write(WalOperation::test_message(i)).unwrap();
            wal.active_segment.flush().unwrap();
        }
        wal.flush().unwrap();

        let (last_segment_id, messages) = Wal::replay(dir.path()).unwrap();
        assert_eq!(last_segment_id, 24);
        assert_eq!(messages.keys().len(), 1, "Only the 'hello' key is expected");

        let contained_messages = messages.get(&Namespace::new("hello")).unwrap().clone();
        assert_eq!(contained_messages.len(), 100);
        for (i, msg) in contained_messages.into_iter().enumerate().take(100) {
            assert_eq!(msg, ServerMessage::test_message(i as i64));
        }

        assert!(
            Wal::try_new(dir.path().to_path_buf(), Some(TINY_SEGMENT_SIZE))
                .ok()
                .is_some_and(|w| w.active_segment.id() == 25),
            "Segment should be +1 from last"
        );
    }

    #[test]
    fn closed_segments() {
        let dir = TempDir::new().unwrap();

        let mut wal = Wal::try_new(dir.path().to_path_buf(), Some(64)).unwrap();

        for i in 0..10 {
            wal.write(WalOperation::test_message(i)).unwrap();
            wal.flush().unwrap();
            wal.rotate().unwrap();
        }
        assert_eq!(wal.closed_segments().len(), 10);

        let wal = Wal::try_new(dir.path().to_path_buf(), None).unwrap();
        assert_eq!(wal.closed_segments().len(), 11); // 0-base index, 0..=10 is 11 closed segments
        assert_eq!(wal.active_segment.id(), 11);
    }

    #[test]
    fn next_segment() {
        let dir = TempDir::new().unwrap();

        let segment_one_path = dir.path().join("1.log");
        let mut segment_one_size = 0;

        let mut s1 = Segment::try_new(1, segment_one_path.clone()).unwrap();
        let (sz, _) = s1.write(WalOperation::test_message(1)).unwrap();
        segment_one_size += sz;
        let (sz, _) = s1.write(WalOperation::test_message(2)).unwrap();
        segment_one_size += sz;
        s1.flush().unwrap();

        assert_eq!(
            segment_one_size as u64,
            std::fs::metadata(&segment_one_path).unwrap().len()
        );

        let segment_two_path = dir.path().join("2.log");
        let mut segment_two_size = 0;
        let (_, mut s2) = s1.next_segment(segment_two_path.clone());
        let (sz, _) = s2.write(WalOperation::test_message(3)).unwrap();
        segment_two_size += sz;
        s2.flush().unwrap();

        assert_eq!(
            segment_one_size as u64,
            std::fs::metadata(&segment_one_path).unwrap().len(),
            "Previous segment should remain unchanged"
        );
        assert_eq!(
            segment_two_size as u64,
            std::fs::metadata(&segment_two_path).unwrap().len()
        );
    }

    #[test]
    fn offset_tracking() {
        let dir = TempDir::new().unwrap();

        let mut w = Wal::try_new(dir.path().to_path_buf(), None).unwrap();
        let (sz, offset) = w.write(WalOperation::test_message(0)).unwrap();
        assert_eq!(
            sz, offset,
            "Offset and size of the write are equal for the first write"
        );

        let mut w = Wal::try_new(dir.path().to_path_buf(), None).unwrap();
        let mut total_write_size = 0;
        let mut expected_offset = 0;
        for _ in 0..10 {
            let (sz, offset) = w.write(WalOperation::test_message(0)).unwrap();
            total_write_size += sz;
            expected_offset = offset;
        }

        assert_eq!(
            total_write_size, expected_offset,
            "Offset is expected to be size of multiple writes"
        );
    }

    #[test]
    fn find_correct_segment_ids() {
        let dir = TempDir::new().unwrap();

        let _segment_paths: Vec<PathBuf> = (1..=5)
            .map(|i| {
                let path = dir.path().join(format!("{i}{WAL_EXTENSION}"));
                // discard the segment, we just need it creating
                Segment::try_new(i, path.clone()).unwrap();
                path
            })
            .collect();

        assert_eq!(find_segment_ids(dir.path()), (1..=5).collect::<Vec<_>>());
    }
}
