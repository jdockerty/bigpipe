use std::io::{self, BufReader, Read};
use std::path::Path;
use std::{fs::File, io::Write, path::PathBuf};

use bytes::{BufMut, Bytes, BytesMut};
use hashbrown::HashMap;
use prost::Message;
use tracing::{debug, error, info};
use walkdir::{DirEntry, WalkDir};

use crate::data_types::proto::SegmentEntry;
use crate::data_types::BigPipeValue;
use crate::ServerMessage;

pub(crate) const DEFAULT_MAX_SEGMENT_SIZE: usize = 16777216; // 16 MiB
const MAX_SEGMENT_BUFFER_SIZE: u16 = 8192; // 8 KiB

const WAL_EXTENSION: &str = "-bp.wal";
pub(crate) const WAL_DEFAULT_ID: u64 = 0;

#[derive(Debug)]
pub struct Wal {
    id: u64,
    active_segment: Segment,
    directory: PathBuf,
}

impl Wal {
    pub fn try_new(
        id: u64,
        directory: PathBuf,
        segment_max_size: Option<usize>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            id,
            directory: directory.clone(),
            active_segment: Segment::try_new(
                directory.join(format!("{id}{WAL_EXTENSION}")),
                segment_max_size,
            )?,
        })
    }

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
                match SegmentEntry::decode(&mut bytes) {
                    Ok(entry) => {
                        messages
                            .entry(entry.key.to_string())
                            .and_modify(|occupied_messages: &mut BigPipeValue| {
                                let entry = entry.clone();
                                occupied_messages.push(ServerMessage::new(
                                    entry.key,
                                    entry.value.into(),
                                    entry.timestamp,
                                ))
                            })
                            .or_insert_with(|| {
                                let mut messages = BigPipeValue::new();
                                messages.push(ServerMessage::new(
                                    entry.key,
                                    entry.value.into(),
                                    entry.timestamp,
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
    pub fn write(&mut self, message: &ServerMessage) -> Result<usize, Box<dyn std::error::Error>> {
        if self.active_segment.current_size >= self.active_segment.max_size {
            self.rotate()?;
        }
        self.active_segment.write(message)
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
        Ok(())
    }

    #[allow(dead_code)]
    pub fn active_segment_path(&self) -> &Path {
        self.active_segment.path()
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
    fn write(&mut self, message: &ServerMessage) -> Result<usize, Box<dyn std::error::Error>> {
        let message_bytes = SegmentEntry {
            key: message.key().to_string(),
            value: message.value().to_vec(),
            timestamp: message.timestamp(),
        }
        .encode_length_delimited_to_vec();
        let size = message_bytes.len();

        self.buf.put_slice(&message_bytes);

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

#[cfg(test)]
mod test {
    use tempfile::TempDir;

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

        wal.write(&ServerMessage::test_message(0)).unwrap();
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

        let mut wal = Wal::try_new(WAL_DEFAULT_ID, dir.path().to_path_buf(), Some(16)).unwrap();

        // Known size of the write
        let server_msg_size = 15;

        for _ in 0..=2 {
            wal.write(&ServerMessage::test_message(0)).unwrap();
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
            wal.write(&ServerMessage::test_message(i)).unwrap();
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
}
