use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs::File, io::Write, path::PathBuf};

use bytes::{BufMut, BytesMut};
use hashbrown::HashMap;
use prost::Message;
use tracing::{debug, info};
use walkdir::DirEntry;

use super::DEFAULT_MAX_SEGMENT_SIZE;
use super::MAX_SEGMENT_BUFFER_SIZE;
use super::WAL_DEFAULT_ID;
use super::WAL_EXTENSION;
use crate::data_types::wal::WalMessageEntry;
use crate::data_types::wal_proto::MessageEntry;
use crate::ServerMessage;

/// A logical offset of an entry.
///
/// For example
///
/// my_namespace => [ messageOne, messageTwo, messageThree ]
///
/// The offset IDs are as follows: messageOne is at 0, messageTwo
/// is at 1, and messageThree is at 2. Essentially, this is
/// an array index into the message queue.
#[derive(Debug)]
struct OffsetId(AtomicU64);

impl OffsetId {
    pub fn new(inner: u64) -> Self {
        OffsetId(AtomicU64::new(inner))
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    pub fn next(&self) -> Self {
        OffsetId((self.0.load(Ordering::Acquire) + 1).into())
    }
}

/// A log implementation for a single namespace,
/// a scoped log.
///
/// The log is used to write bigpipe operations
/// into an append-only file for durability, before
/// becoming available to consumers.
#[derive(Debug)]
pub struct ScopedLog {
    /// The current active [`Segment`] file.
    ///
    /// Incoming messages are written to this file
    /// up until reaching `segment_max_size`.
    active_segment: Segment,
    /// The current logical offset to apply to an
    /// incoming message.
    current_offset: OffsetId,
    /// In-memory index for tracking logical offset
    /// IDs to a segment and physical byte offset.
    offset_index: HashMap<u64, (SegmentId, usize)>,
    /// Max size of a segment before it is closed
    /// and a new segment is opened.
    segment_max_size: usize,
    /// Collection of closed segment IDs.
    closed_segments: Vec<SegmentId>,
    /// Directory configured for this [`ScopedLog`].
    directory: PathBuf,
}

impl ScopedLog {
    pub fn try_new(
        directory: PathBuf,
        segment_max_size: Option<usize>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let closed_segments = find_segment_ids(&directory);

        // Segments are sorted, the last element is the largest current segment.
        #[expect(clippy::bind_instead_of_map)]
        let id = closed_segments
            .last()
            .and_then(|s| Some(s + 1))
            .unwrap_or(WAL_DEFAULT_ID);

        Ok(Self {
            current_offset: OffsetId::new(0),
            offset_index: HashMap::new(),
            directory: directory.clone(),
            segment_max_size: segment_max_size.unwrap_or(DEFAULT_MAX_SEGMENT_SIZE),
            active_segment: Segment::try_new(id, directory.join(format!("{id}{WAL_EXTENSION}")))?,
            closed_segments,
        })
    }

    /// Write a message into the write-ahead log.
    pub fn write(
        &mut self,
        op: WalMessageEntry,
    ) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        if self.active_segment.current_size >= self.segment_max_size {
            self.rotate()?;
        }
        let (sz, byte_offset) = self.active_segment.write(op, self.current_offset.get())?;
        self.offset_index.insert(
            self.current_offset.get(),
            (self.active_segment.id.clone(), byte_offset),
        );
        self.current_offset = self.current_offset.next();
        Ok((sz, byte_offset))
    }

    pub fn read(&self, offset: u64) -> io::Result<Option<Vec<ServerMessage>>> {
        let (segment_id, byte_offset) = match self.offset_index.get(&offset) {
            Some(x) => x,
            None => return Ok(None),
        };

        let file = std::fs::File::open(
            self.directory
                .join(format!("{}{WAL_EXTENSION}", segment_id.0)),
        )?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(*byte_offset as u64))?;

        let mut out = Vec::new();
        loop {
            let mut sz = [0u8; 8]; // u64 len delimiter
            match reader.read_exact(&mut sz) {
                Ok(_) => {}
                Err(_e) => break,
            };

            // Read the message bytes
            let mut bytes = vec![0u8; u64::from_le_bytes(sz) as usize];
            reader.read_exact(&mut bytes)?;

            let mut bytes = bytes::BytesMut::from(&bytes[..]);
            let msg = MessageEntry::decode(&mut bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            out.push(ServerMessage::new(msg.key, msg.value.into(), msg.timestamp));
        }
        Ok(Some(out))
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

    /// Perform a write into the [`Segment`], returning the number of bytes written
    /// and the physical byte offset message.
    fn write(
        &mut self,
        msg: WalMessageEntry,
        offset: u64,
    ) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        let message = MessageEntry {
            key: msg.key,
            value: msg.value.into(),
            timestamp: msg.timestamp,
            offset,
        };
        let message_bytes = message.encode_to_vec();
        let len = message_bytes.len();

        // The byte offset is the end of the current
        // file. This is BEFORE we write the incoming
        // message.
        let byte_offset = self.current_size;

        self.buf.put_u64_le(len as u64);
        self.buf.put_slice(&message_bytes);

        // Size is 8 bytes (u64 len delimiter) + message byte len
        let size = 8 + message_bytes.len();
        self.current_size += size;

        if self.buf.len() >= MAX_SEGMENT_BUFFER_SIZE as usize {
            self.flush()?;
        }

        Ok((size, byte_offset))
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

        let mut wal = ScopedLog::try_new(dir.path().to_path_buf(), None).unwrap();

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

        let mut wal = ScopedLog::try_new(dir.path().to_path_buf(), None).unwrap();

        wal.write(WalMessageEntry::test_message(0, 0)).unwrap();
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

        let mut wal = ScopedLog::try_new(dir.path().to_path_buf(), None).unwrap();

        let mut total = 0;
        for i in 0..=2 {
            let (sz, _) = wal.write(WalMessageEntry::test_message(0, i)).unwrap();
            total += sz;
            wal.flush().unwrap();
        }
        assert_eq!(
            wal.active_segment.file.metadata().unwrap().len(),
            total as u64,
        );
        assert_eq!(wal.active_segment.id(), 0);

        wal.rotate().unwrap();
        assert_eq!(wal.active_segment.id(), 1, "Expected rotation");

        assert_eq!(wal.active_segment.file.metadata().unwrap().len(), 0);
        assert_eq!(
            std::fs::File::open(dir.path().join(format!("{WAL_DEFAULT_ID}{WAL_EXTENSION}")))
                .unwrap()
                .metadata()
                .unwrap()
                .len(),
            total as u64
        )
    }

    // #[test]
    // fn replay() {
    //     let dir = TempDir::new().unwrap();

    //     const TINY_SEGMENT_SIZE: usize = 64;
    //     let mut wal = ScopedLog::try_new(dir.path().to_path_buf(), Some(TINY_SEGMENT_SIZE)).unwrap();

    //     for i in 0..100 {
    //         wal.write(WalMessageEntry::test_message(i)).unwrap();
    //         wal.active_segment.flush().unwrap();
    //     }
    //     wal.flush().unwrap();

    //     let (last_segment_id, messages) = Wal::replay(dir.path()).unwrap();
    //     assert_eq!(last_segment_id, 24);
    //     assert_eq!(messages.keys().len(), 1, "Only the 'hello' key is expected");

    //     let contained_messages = messages.get(&Namespace::new("hello")).unwrap().clone();
    //     assert_eq!(contained_messages.len(), 100);
    //     for (i, msg) in contained_messages.into_iter().enumerate().take(100) {
    //         assert_eq!(msg, ServerMessage::test_message(i as i64));
    //     }

    //     assert!(
    //         ScopedLog::try_new(dir.path().to_path_buf(), Some(TINY_SEGMENT_SIZE))
    //             .ok()
    //             .is_some_and(|w| w.active_segment.id() == 25),
    //         "Segment should be +1 from last"
    //     );
    // }

    #[test]
    fn closed_segments() {
        let dir = TempDir::new().unwrap();

        let mut wal = ScopedLog::try_new(dir.path().to_path_buf(), Some(64)).unwrap();

        for i in 0..10 {
            wal.write(WalMessageEntry::test_message(i, i as u64))
                .unwrap();
            wal.flush().unwrap();
            wal.rotate().unwrap();
        }
        assert_eq!(wal.closed_segments().len(), 10);

        let wal = ScopedLog::try_new(dir.path().to_path_buf(), None).unwrap();
        assert_eq!(wal.closed_segments().len(), 11); // 0-base index, 0..=10 is 11 closed segments
        assert_eq!(wal.active_segment.id(), 11);
    }

    #[test]
    fn next_segment() {
        let dir = TempDir::new().unwrap();

        let segment_one_path = dir.path().join("1.log");
        let mut segment_one_size = 0;

        let mut s1 = Segment::try_new(1, segment_one_path.clone()).unwrap();
        let (sz, _) = s1.write(WalMessageEntry::test_message(0, 0), 0).unwrap();
        segment_one_size += sz;
        let (sz, _) = s1.write(WalMessageEntry::test_message(1, 1), 1).unwrap();
        segment_one_size += sz;
        s1.flush().unwrap();

        assert_eq!(
            segment_one_size as u64,
            std::fs::metadata(&segment_one_path).unwrap().len()
        );

        let segment_two_path = dir.path().join("2.log");
        let mut segment_two_size = 0;
        let (_, mut s2) = s1.next_segment(segment_two_path.clone());
        let (sz, _) = s2.write(WalMessageEntry::test_message(2, 2), 2).unwrap();
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

        let mut w = ScopedLog::try_new(dir.path().to_path_buf(), None).unwrap();
        assert_eq!(
            w.current_offset.get(),
            0,
            "Logical offset should start at 0"
        );

        w.write(WalMessageEntry::test_message(0, 0)).unwrap();
        assert_eq!(
            w.current_offset.get(),
            1,
            "Writing a message should increment the logical offset"
        );

        let mut w = ScopedLog::try_new(dir.path().to_path_buf(), None).unwrap();
        for i in 1..=10 {
            w.write(WalMessageEntry::test_message(0, i)).unwrap();
        }
        assert_eq!(w.current_offset.get(), 10);
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

    #[test]
    fn read_from_log() {
        let dir = TempDir::new().unwrap();

        let mut w = ScopedLog::try_new(dir.path().to_path_buf(), None).unwrap();

        let messages = (1..=10)
            .map(|i| WalMessageEntry::test_message(i, (i - 1) as u64))
            .collect::<Vec<_>>();

        for m in messages.clone() {
            w.write(m).unwrap();
            w.flush().unwrap();
        }
        assert_eq!(w.offset_index.len(), 10);

        let messages_read = w.read(0).unwrap().expect("read some messages");
        for (written, read) in std::iter::zip(messages.clone(), messages_read) {
            assert_eq!(
                written.timestamp,
                read.timestamp(),
                "Timestamps should match exactly in order"
            );
            // Offsets are known to be timestamp - 1 on creation for this test.
            let offset = written.timestamp - 1;
            assert_eq!(
                written.offset, offset as u64,
                "Offsets should match exactly in order"
            );
        }

        assert!(
            w.read(50).unwrap().is_none(),
            "Reading a non-existent offset should result in None"
        );
        let messages_read = w.read(6).unwrap().expect("read some messages");
        for (written, read) in std::iter::zip(&messages[6..10], messages_read) {
            assert_eq!(
                written.timestamp,
                read.timestamp(),
                "Timestamps should match exactly in order"
            );
        }
    }
}
