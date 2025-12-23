use std::{
    collections::BTreeMap,
    future::Future,
    io::{Error, ErrorKind, Result},
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

use attic::api::v1::upload_path::UploadPathResult;
use tokio::{
    fs::File,
    io::{AsyncRead, ReadBuf},
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::error::ServerResult;

#[derive(Debug)]
pub struct ActiveUploadSession {
    pub sender: Sender<PartMessage>,
    pub result_handle: JoinHandle<ServerResult<UploadPathResult>>,
    pub username: Option<String>,
    pub cache_name: String,
}

#[derive(Debug)]
pub enum PartMessage {
    Part { seq: u64, path: PathBuf },
    Abort,
}

type OpenFuture = Pin<Box<dyn Future<Output = Result<File>> + Send>>;

enum ReaderState {
    Idle,
    Opening { future: OpenFuture },
    Reading { file: File },
}

enum Progress {
    DataRead,
    StateAdvanced,
    Pending,
    Done,
}

pub struct OrderedPartReader {
    receiver: Receiver<PartMessage>,
    pending: BTreeMap<u64, PathBuf>,
    next_seq: u64,
    state: ReaderState,
    current_path: Option<PathBuf>,
    channel_closed: bool,
    max_pending: usize,
}

impl OrderedPartReader {
    #[must_use]
    pub fn new(receiver: Receiver<PartMessage>, max_pending: usize) -> Self {
        OrderedPartReader {
            receiver,
            pending: BTreeMap::new(),
            next_seq: 0,
            state: ReaderState::Idle,
            current_path: None,
            channel_closed: false,
            max_pending,
        }
    }

    fn open_part(&mut self, path: PathBuf) {
        tracing::debug!(seq = self.next_seq, path = %path.display(), "Opening part file");
        let path_clone = path.clone();
        let future = Box::pin(async move { File::open(&path_clone).await });
        self.state = ReaderState::Opening { future };
        self.current_path = Some(path);
    }

    fn finish_part(&mut self) {
        tracing::debug!(
            seq = self.next_seq,
            "Finished reading part, advancing to next"
        );
        self.next_seq += 1;
        self.state = ReaderState::Idle;
        if let Some(path) = self.current_path.take() {
            self.discard_part(path);
        }
    }

    fn discard_part(&mut self, path: PathBuf) {
        tracing::debug!(path = %path.display(), "Discarding part file");
        tokio::spawn(async move {
            if let Err(e) = tokio::fs::remove_file(&path).await {
                tracing::warn!("Failed to remove part file {}: {e}", path.display());
            }
        });
    }

    fn poll_idle(&mut self, cx: &mut Context<'_>) -> Result<Progress> {
        tracing::trace!(
            next_seq = self.next_seq,
            pending_count = self.pending.len(),
            channel_closed = self.channel_closed,
            "Polling idle state"
        );
        if let Some(path) = self.pending.remove(&self.next_seq) {
            tracing::debug!(seq = self.next_seq, "Found pending part, opening");
            self.open_part(path);
            return Ok(Progress::StateAdvanced);
        }

        if self.channel_closed {
            if self.pending.is_empty() {
                return Ok(Progress::Done);
            } else {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    format!(
                        "Missing parts in stream. Next expected part: {}",
                        self.next_seq
                    ),
                ));
            }
        }

        loop {
            match Pin::new(&mut self.receiver).poll_recv(cx) {
                Poll::Ready(Some(msg)) => {
                    tracing::debug!("Received message from channel");
                    match self.handle_message(msg) {
                        Ok(Progress::StateAdvanced) => return Ok(Progress::StateAdvanced),
                        Ok(Progress::Pending) => continue,
                        Err(e) => return Err(e),
                        Ok(res) => return Ok(res),
                    }
                }
                Poll::Ready(None) => {
                    tracing::debug!("Channel closed, no more parts expected");
                    self.channel_closed = true;
                    return Ok(Progress::StateAdvanced);
                }
                Poll::Pending => return Ok(Progress::Pending),
            }
        }
    }

    fn handle_message(&mut self, msg: PartMessage) -> Result<Progress> {
        match msg {
            PartMessage::Part { seq, path } => {
                tracing::debug!(seq, path = %path.display(), "Handling part message");
                self.handle_part(seq, path)
            }
            PartMessage::Abort => {
                tracing::debug!("Received abort message, discarding all pending parts");
                self.discard_all_pending();
                Err(Error::new(
                    ErrorKind::Interrupted,
                    "Upload aborted by client",
                ))
            }
        }
    }

    fn discard_all_pending(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        let count = pending.len();
        tracing::debug!(count, "Discarding all pending parts");
        tokio::spawn(async move {
            for (seq, path) in pending {
                tracing::debug!(seq, path = %path.display(), "Removing pending part file");
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    tracing::warn!("Failed to remove pending part file {}: {e}", path.display());
                }
            }
        });
    }

    fn handle_part(&mut self, seq: u64, path: PathBuf) -> Result<Progress> {
        if seq == self.next_seq {
            tracing::debug!(seq, "Part matches expected sequence, opening immediately");
            self.open_part(path);
            Ok(Progress::StateAdvanced)
        } else if seq > self.next_seq {
            if self.pending.len() >= self.max_pending {
                tracing::debug!(
                    seq,
                    max_pending = self.max_pending,
                    "Too many out-of-order parts pending, rejecting"
                );
                return Err(Error::new(
                    ErrorKind::Other,
                    "Too many out-of-order parts pending",
                ));
            }
            tracing::debug!(
                seq,
                next_seq = self.next_seq,
                pending_count = self.pending.len(),
                "Part arrived out of order, buffering"
            );
            self.pending.entry(seq).or_insert(path);
            Ok(Progress::Pending)
        } else {
            tracing::warn!("Received old part {seq}, expected {}", self.next_seq);
            self.discard_part(path);
            Ok(Progress::Pending)
        }
    }
}

impl AsyncRead for OrderedPartReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let progress = match &mut self.state {
            ReaderState::Idle => self.poll_idle(cx),
            ReaderState::Opening { future } => match future.as_mut().poll(cx) {
                Poll::Ready(Ok(file)) => {
                    tracing::debug!(
                        seq = self.next_seq,
                        "Part file opened successfully, starting read"
                    );
                    self.state = ReaderState::Reading { file };
                    Ok(Progress::StateAdvanced)
                }
                Poll::Ready(Err(e)) => {
                    tracing::debug!(seq = self.next_seq, error = %e, "Failed to open part file");
                    self.state = ReaderState::Idle;
                    if let Some(path) = self.current_path.take() {
                        self.discard_part(path);
                    }
                    Err(e)
                }
                Poll::Pending => Ok(Progress::Pending),
            },
            ReaderState::Reading { file } => {
                let before = buf.filled().len();
                match Pin::new(file).poll_read(cx, buf) {
                    Poll::Ready(Ok(())) => {
                        let bytes_read = buf.filled().len() - before;
                        if bytes_read == 0 {
                            tracing::debug!(seq = self.next_seq, "Reached end of part file");
                            self.finish_part();
                            Ok(Progress::StateAdvanced)
                        } else {
                            tracing::trace!(
                                seq = self.next_seq,
                                bytes_read,
                                "Read data from part file"
                            );
                            Ok(Progress::DataRead)
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        tracing::debug!(seq = self.next_seq, error = %e, "Error reading part file");
                        Err(e)
                    }
                    Poll::Pending => Ok(Progress::Pending),
                }
            }
        };

        match progress {
            Ok(Progress::Done) => {
                tracing::debug!("Stream completed successfully");
                Poll::Ready(Ok(()))
            }
            Ok(Progress::DataRead) => Poll::Ready(Ok(())),
            Ok(Progress::StateAdvanced) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Ok(Progress::Pending) => Poll::Pending,
            Err(e) => {
                tracing::debug!(error = %e, "Stream encountered error");
                Poll::Ready(Err(e))
            }
        }
    }
}
