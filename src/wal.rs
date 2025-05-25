use std::convert::TryInto;
use std::path::Path;
use std::{fs::File, io::Write, path::PathBuf};

use crate::ServerMessage;

pub(crate) const DEFAULT_MAX_SEGMENT_SIZE: usize = 16777216; // 16 MiB
const MAX_SEGMENT_BUFFER_SIZE: u16 = 8192; // 8 KiB

const WAL_EXTENSION: &str = "-bp.wal";
const WAL_DEFAULT_ID: u64 = 0;

pub struct Wal {
    id: u64,
    active_segment: Segment,
    directory: PathBuf,
}

impl Wal {
    pub fn new(directory: PathBuf, segment_max_size: Option<usize>) -> Self {
        // TODO: detect pre-existing segments, for now it always
        // starts at 0.
        Self {
            id: WAL_DEFAULT_ID,
            directory: directory.clone(),
            active_segment: Segment::new(
                directory.join(format!("{WAL_DEFAULT_ID}{WAL_EXTENSION}")),
                segment_max_size,
            ),
        }
    }

    /// Write a message into the write-ahead log.
    pub fn write(&mut self, message: ServerMessage) -> Result<(), Box<dyn std::error::Error>> {
        Ok(self.active_segment.write(message)?)
    }

    /// Force flush the current active segment.
    pub(crate) fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(self.active_segment.flush()?)
    }

    pub fn active_segment_path(&self) -> &Path {
        self.active_segment.path()
    }
}

struct Segment {
    filepath: PathBuf,
    file: File,
    file_max_size: usize,
    buf: Vec<u8>,
}

impl Segment {
    fn new(segment_path: PathBuf, segment_max_size: Option<usize>) -> Self {
        let buf = Vec::with_capacity(MAX_SEGMENT_BUFFER_SIZE as usize);
        Self {
            filepath: segment_path.clone(),
            file: File::create_new(segment_path).unwrap(),
            file_max_size: segment_max_size.unwrap_or(DEFAULT_MAX_SEGMENT_SIZE as usize),
            buf,
        }
    }

    fn write(&mut self, message: ServerMessage) -> Result<(), Box<dyn std::error::Error>> {
        let message_bytes: Vec<u8> = message.try_into()?;

        self.buf.write_all(&message_bytes)?;

        if self.buf.len() >= MAX_SEGMENT_BUFFER_SIZE as usize {
            self.flush()?;
        }

        Ok(self.file.write_all(&message_bytes)?)
    }

    fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.file.write_all(&self.buf)?;
        self.file.sync_all()?;
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

        let wal = Wal::new(dir.path().to_path_buf(), None);

        let expected_path = dir.path().join(format!("{WAL_DEFAULT_ID}{WAL_EXTENSION}"));
        assert!(expected_path.exists());
        assert_eq!(wal.active_segment.path(), &expected_path);

        // TODO: wal rotiation increases segment ID
    }

    #[test]
    fn write() {
        let dir = TempDir::new().unwrap();

        let mut wal = Wal::new(dir.path().to_path_buf(), None);

        wal.write(ServerMessage {
            key: "hello".to_string(),
            value: "world".into(),
            timestamp: 1,
        })
        .unwrap();

        wal.flush().unwrap();

        assert!(wal.active_segment_path().exists());
        assert_ne!(
            wal.active_segment.file.metadata().unwrap().len(),
            0,
            "Flushing WAL expected a non-zero file size after writing"
        );
    }
}
