use std::io::{self, BufReader, Read};
use std::path::Path;
use std::{fs::File, io::Write, path::PathBuf};

use bytes::{BufMut, Bytes, BytesMut};
use hashbrown::HashMap;
use prost::Message;
use tracing::{debug, error, info};
use walkdir::{DirEntry, WalkDir};

use super::DEFAULT_MAX_SEGMENT_SIZE;
use super::MAX_SEGMENT_BUFFER_SIZE;
use super::WAL_DEFAULT_ID;
use super::WAL_EXTENSION;
use crate::data_types::wal::segment_entry::Entry as EntryProto;
use crate::data_types::wal::SegmentEntry as SegmentEntryProto;
use crate::data_types::{BigPipeValue, RetentionPolicy, WalOperation};
use crate::ServerMessage;

/// A write-ahead log (WAL) implementation.
///
/// The WAL is used to write bigpipe operations
/// into an append-only file for durability, before
/// becoming available to consumers.
#[derive(Debug)]
pub struct Wal {
    id: u64,
    active_segment: Segment,
    closed_segments: Vec<u64>,
    directory: PathBuf,
}

impl Wal {
    pub fn try_new(
        id: u64,
        directory: PathBuf,
        segment_max_size: Option<usize>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // When non-zero segments are provided, a system invariant
        // here assumes that the IDs are monotonically increasing.
        //
        // In other words, if '5' is given, segments 0..=4 MUST
        // already exists.
        let closed_segments = (0..id).collect::<Vec<_>>();

        Ok(Self {
            id,
            directory: directory.clone(),
            active_segment: Segment::try_new(
                directory.join(format!("{id}{WAL_EXTENSION}")),
                segment_max_size,
            )?,
            closed_segments,
        })
    }

    /// Replay an individual [`Wal`], returning the inner data representation.
    /// This requires reading ALL segments that are available.
    pub fn replay<P: AsRef<Path>>(directory: P) -> Option<(u64, HashMap<String, BigPipeValue>)> {
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
                    Ok(entry) => match entry.entry.expect("segment entry is encoded") {
                        EntryProto::MessageEntry(message_entry) => {
                            messages
                                .entry(message_entry.key.to_string())
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
                        EntryProto::NamespaceEntry(namespace) => {
                            messages
                                .entry(namespace.key)
                                .and_modify(|v| {
                                    v.set_retention_policy(
                                        RetentionPolicy::try_from(namespace.retention_policy)
                                            .unwrap(),
                                    )
                                })
                                .or_insert_with(|| {
                                    let mut value = BigPipeValue::new();
                                    value.set_retention_policy(
                                        RetentionPolicy::try_from(namespace.retention_policy)
                                            .unwrap(),
                                    );
                                    value
                                });
                        }
                    },
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
    pub fn write(&mut self, op: WalOperation) -> Result<usize, Box<dyn std::error::Error>> {
        if self.active_segment.current_size >= self.active_segment.max_size {
            self.rotate()?;
        }
        self.active_segment.write(op)
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

        let current_id = self.id;
        self.id += 1;
        let next_id = self.id;
        let next_path = self.directory.join(format!("{}{WAL_EXTENSION}", next_id));

        info!(
            current_id,
            next_id,
            next_path = %next_path.to_string_lossy(),
            current_size = self.active_segment.current_size,
            max_size = self.active_segment.max_size,
            "rotating wal segment"
        );

        self.active_segment = Segment::try_new(next_path, Some(self.active_segment.max_size))?;
        // Push the previously held 'current' only once the new segment is opened
        self.closed_segments.push(current_id);
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

#[derive(Debug)]
struct Segment {
    filepath: PathBuf,
    file: File,
    current_size: usize,
    max_size: usize,
    buf: BytesMut,
}

impl Segment {
    /// Construct a new [`Segment`].
    fn try_new(
        segment_path: PathBuf,
        segment_max_size: Option<usize>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let buf = BytesMut::with_capacity(MAX_SEGMENT_BUFFER_SIZE as usize);
        Ok(Self {
            filepath: segment_path.clone(),
            file: File::create_new(segment_path)?,
            max_size: segment_max_size.unwrap_or(DEFAULT_MAX_SEGMENT_SIZE),
            current_size: 0,
            buf,
        })
    }

    /// Perform a write into the [`Segment`], returning the number of bytes written.
    fn write(&mut self, op: WalOperation) -> Result<usize, Box<dyn std::error::Error>> {
        let wal_operation = SegmentEntryProto::try_from(op)
            .unwrap()
            .encode_length_delimited_to_vec();
        let size = wal_operation.len();

        self.buf.put_slice(&wal_operation);

        if self.buf.len() >= MAX_SEGMENT_BUFFER_SIZE as usize {
            self.flush()?;
        }

        Ok(size)
    }

    /// Flush data for the [`Segment`] from the internal buffer to the underlying
    /// file.
    fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("flushing internal segment buffer");
        self.file.write_all(&self.buf)?;
        self.file.sync_all()?;
        self.current_size += self.buf.len();
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

    use crate::data_types::{RetentionPolicy, WalNamespaceEntry};

    use super::*;

    #[test]
    fn path_semantics() {
        let dir = TempDir::new().unwrap();

        let mut wal = Wal::try_new(WAL_DEFAULT_ID, dir.path().to_path_buf(), None).unwrap();

        let expected_path = dir.path().join(format!("{WAL_DEFAULT_ID}{WAL_EXTENSION}"));
        assert!(expected_path.exists());
        assert_eq!(wal.active_segment.path(), &expected_path);
        assert_eq!(wal.id, 0);

        wal.rotate().unwrap();
        let expected_path = dir.path().join(format!("1{WAL_EXTENSION}"));
        assert!(expected_path.exists());
        assert_eq!(wal.active_segment.path(), &expected_path);
        assert_eq!(wal.id, 1);
    }

    #[test]
    fn write() {
        let dir = TempDir::new().unwrap();

        let mut wal = Wal::try_new(WAL_DEFAULT_ID, dir.path().to_path_buf(), None).unwrap();

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

        let mut wal = Wal::try_new(WAL_DEFAULT_ID, dir.path().to_path_buf(), Some(24)).unwrap();

        // Known size of the write
        let server_msg_size = 17;

        for _ in 0..=2 {
            wal.write(WalOperation::test_message(0)).unwrap();
            wal.flush().unwrap();
        }

        assert_eq!(wal.id, 1, "Expected rotation");

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
        let mut wal = Wal::try_new(
            WAL_DEFAULT_ID,
            dir.path().to_path_buf(),
            Some(TINY_SEGMENT_SIZE),
        )
        .unwrap();

        for i in 0..100 {
            wal.write(WalOperation::test_message(i)).unwrap();
            wal.active_segment.flush().unwrap();
        }
        wal.flush().unwrap();

        let (last_segment_id, messages) = Wal::replay(dir.path()).unwrap();
        assert_eq!(last_segment_id, 24);
        assert_eq!(messages.keys().len(), 1, "Only the 'hello' key is expected");

        let contained_messages = messages.get("hello").unwrap().clone();
        assert_eq!(contained_messages.len(), 100);
        for (i, msg) in contained_messages.into_iter().enumerate().take(100) {
            assert_eq!(msg, ServerMessage::test_message(i as i64));
        }

        assert!(
            Wal::try_new(
                last_segment_id,
                dir.path().to_path_buf(),
                Some(TINY_SEGMENT_SIZE),
            )
            .is_err(),
            "Creating segment with same ID is not allowed"
        );

        assert!(
            Wal::try_new(
                last_segment_id + 1,
                dir.path().to_path_buf(),
                Some(TINY_SEGMENT_SIZE),
            )
            .is_ok(),
            "Segment should be +1 from last"
        );
    }

    #[test]
    fn namespace_creation_durable() {
        let dir = TempDir::new().unwrap();

        let mut wal = Wal::try_new(WAL_DEFAULT_ID, dir.path().to_path_buf(), None).unwrap();
        let namespace = "my_new_namespace";

        wal.write(WalOperation::Namespace(WalNamespaceEntry {
            key: namespace.to_string(),
            retention_policy: RetentionPolicy::Ttl, // using the non-default policy
        }))
        .unwrap();
        wal.flush().unwrap();

        drop(wal);

        let (_, inner) = Wal::replay(dir.path()).unwrap();
        assert_eq!(inner.keys().len(), 1);

        let mut expected = BigPipeValue::new();
        expected.set_retention_policy(RetentionPolicy::Ttl);

        let got = inner.get(namespace).unwrap().clone();
        assert_eq!(got.is_empty(), expected.is_empty());
        assert!(
            got.get(0).is_none(),
            "Namespace was only created, no values are expected"
        );
        assert_eq!(got.retention_policy(), expected.retention_policy());

        // TODO: expand with deletion:
        //  - adding entries shows those entries on replay
        //  - removal yields no namespace at all
    }

    #[test]
    fn closed_segments() {
        let dir = TempDir::new().unwrap();

        let mut wal = Wal::try_new(WAL_DEFAULT_ID, dir.path().to_path_buf(), Some(64)).unwrap();

        for i in 0..10 {
            wal.write(WalOperation::test_message(i)).unwrap();
            wal.flush().unwrap();
        }
        assert_eq!(wal.closed_segments().len(), 2);

        let wal = Wal::try_new(10, dir.path().to_path_buf(), None).unwrap();
        assert_eq!(wal.closed_segments().len(), 10); // 0-base index, 0..=9 is 10 closed segments
    }
}
