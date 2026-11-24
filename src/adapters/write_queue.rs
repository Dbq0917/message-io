use std::collections::VecDeque;
use std::io::{self, ErrorKind, Write};

/// Default maximum amount of userspace data retained for one slow TCP connection.
///
/// The limit is measured in bytes because file chunks and other messages can have
/// very different sizes. A single `send()` larger than this value can still add one
/// message-sized overshoot; subsequent sends are rejected with backpressure until it drains.
pub(crate) const DEFAULT_PENDING_SEND_LIMIT: usize = 1024 * 1024;

#[derive(Debug)]
struct PendingChunk {
    bytes: Vec<u8>,
    offset: usize,
}

/// Bounded FIFO of bytes that could not be written to a non-blocking socket yet.
#[derive(Debug)]
pub(crate) struct WriteQueue {
    chunks: VecDeque<PendingChunk>,
    pending_bytes: usize,
    limit: usize,
}

impl Default for WriteQueue {
    fn default() -> Self {
        Self::new(DEFAULT_PENDING_SEND_LIMIT)
    }
}

impl WriteQueue {
    pub(crate) fn new(limit: usize) -> Self {
        Self { chunks: VecDeque::new(), pending_bytes: 0, limit: limit.max(1) }
    }

    pub(crate) fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// Returns true when the high-water limit has been reached.
    /// A caller can still append one chunk after this check, so the hard memory bound is
    /// `limit + one send payload + frame headers`.
    pub(crate) fn is_full(&self) -> bool {
        self.pending_bytes >= self.limit
    }

    pub(crate) fn enqueue(&mut self, bytes: Vec<u8>) {
        self.pending_bytes += bytes.len();
        self.chunks.push_back(PendingChunk { bytes, offset: 0 });
    }

    /// Writes as much queued data as possible in FIFO order.
    /// Returns `Ok(true)` when the queue is empty and `Ok(false)` when the writer would block.
    pub(crate) fn flush<W: Write>(&mut self, writer: &mut W) -> io::Result<bool> {
        while let Some(chunk) = self.chunks.front_mut() {
            let remaining = &chunk.bytes[chunk.offset..];
            match writer.write(remaining) {
                Ok(0) => {
                    return Err(io::Error::new(ErrorKind::WriteZero, "write returned zero"));
                }
                Ok(written) => {
                    chunk.offset += written;
                    self.pending_bytes = self.pending_bytes.saturating_sub(written);
                    if chunk.offset == chunk.bytes.len() {
                        self.chunks.pop_front();
                    }
                }
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) if err.kind() == ErrorKind::WouldBlock => return Ok(false),
                Err(err) => return Err(err),
            }
        }

        debug_assert_eq!(self.pending_bytes, 0);
        Ok(true)
    }
}

/// Writes the prefix of `data` accepted by the OS without active waiting.
/// The unwritten suffix is returned so the caller can queue it or report backpressure.
pub(crate) fn write_prefix<'a, W: Write>(
    writer: &mut W,
    data: &'a [u8],
) -> io::Result<Option<&'a [u8]>> {
    let mut written_total = 0;
    while written_total < data.len() {
        match writer.write(&data[written_total..]) {
            Ok(0) => {
                return Err(io::Error::new(ErrorKind::WriteZero, "write returned zero"));
            }
            Ok(written) => written_total += written,
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) if err.kind() == ErrorKind::WouldBlock => {
                return Ok(Some(&data[written_total..]));
            }
            Err(err) => return Err(err),
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SlowWriter {
        written: Vec<u8>,
        budget: usize,
    }

    impl Write for SlowWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.budget == 0 {
                return Err(io::Error::new(ErrorKind::WouldBlock, "budget exhausted"));
            }

            let count = buf.len().min(self.budget);
            self.written.extend_from_slice(&buf[..count]);
            self.budget -= count;
            Ok(count)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn flushes_partial_writes_in_fifo_order() {
        let mut queue = WriteQueue::new(1024);
        queue.enqueue(vec![1, 2, 3, 4, 5]);
        queue.enqueue(vec![6, 7]);

        let mut writer = SlowWriter { written: Vec::new(), budget: 3 };
        assert!(!queue.flush(&mut writer).unwrap());
        assert_eq!(writer.written, vec![1, 2, 3]);
        assert_eq!(queue.pending_bytes(), 4);

        writer.budget = 10;
        assert!(queue.flush(&mut writer).unwrap());
        assert_eq!(writer.written, vec![1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(queue.pending_bytes(), 0);
    }

    #[test]
    fn write_prefix_returns_unwritten_suffix() {
        let mut writer = SlowWriter { written: Vec::new(), budget: 4 };
        let data = [0u8; 10];

        let remaining = write_prefix(&mut writer, &data).unwrap().expect("should block");
        assert_eq!(remaining.len(), 6);
        assert_eq!(writer.written.len(), 4);

        writer.budget = 10;
        assert!(write_prefix(&mut writer, remaining).unwrap().is_none());
        assert_eq!(writer.written.len(), 10);
    }
}
