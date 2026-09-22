//! Accept loop and per-connection pipeline: length-prefixed frames in, requests handled concurrently,
//! produces in arrival order, responses written back in request order.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::ApiKey;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

use crate::error::Error;
use crate::{Kafka, dispatch};

const LENGTH_PREFIX: usize = 4;

pub(crate) async fn accept(kafka: Kafka, listener: TcpListener, mut stop: watch::Receiver<bool>) {
    loop {
        let (socket, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(e) => {
                    tracing::warn!(%e, "kafka accept failed");
                    continue;
                }
            },
            _ = stop.changed() => return,
        };
        let kafka = kafka.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            match connection(kafka, socket, stop).await {
                Ok(()) => tracing::debug!(%peer, "kafka client disconnected"),
                Err(e) => tracing::debug!(%peer, %e, "kafka connection closed"),
            }
        });
    }
}

enum Reply {
    Frame(Bytes),
    Silent,
    Close,
}

struct Slot {
    reply: Reply,
    _permit: OwnedSemaphorePermit,
}

async fn connection(
    kafka: Kafka,
    socket: TcpStream,
    mut stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    socket.set_nodelay(true)?;
    let (reader, writer) = socket.into_split();
    let in_flight = kafka.config().max_in_flight.max(1);
    let (tx, rx) = mpsc::channel(in_flight);
    let permits = Arc::new(Semaphore::new(in_flight));
    let mut writer = tokio::spawn(ordered(writer, rx));

    tokio::select! {
        read = requests(kafka, reader, tx, permits) => {
            let _ = writer.await;
            read
        }
        _ = &mut writer => Ok(()),
        _ = stop.changed() => Ok(()),
    }
}

async fn requests(
    kafka: Kafka,
    mut reader: OwnedReadHalf,
    replies: mpsc::Sender<(u64, Slot)>,
    permits: Arc<Semaphore>,
) -> Result<(), Error> {
    let (produce_tx, produce_rx) = mpsc::channel(permits.available_permits().max(1));
    tokio::spawn(produces(kafka.clone(), produce_rx, replies.clone()));
    let max = kafka.config().max_request_bytes;

    for sequence in 0u64.. {
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        let Some(frame) = read_frame(&mut reader, max).await? else {
            return Ok(());
        };
        if api_key(&frame) == Some(ApiKey::Produce as i16) {
            if produce_tx.send((sequence, frame, permit)).await.is_err() {
                return Ok(());
            }
            continue;
        }
        let kafka = kafka.clone();
        let replies = replies.clone();
        tokio::spawn(async move {
            let reply = reply(&kafka, frame).await;
            let _ = replies
                .send((
                    sequence,
                    Slot {
                        reply,
                        _permit: permit,
                    },
                ))
                .await;
        });
    }

    Ok(())
}

async fn produces(
    kafka: Kafka,
    mut frames: mpsc::Receiver<(u64, Bytes, OwnedSemaphorePermit)>,
    replies: mpsc::Sender<(u64, Slot)>,
) {
    while let Some((sequence, frame, permit)) = frames.recv().await {
        let reply = reply(&kafka, frame).await;
        if replies
            .send((
                sequence,
                Slot {
                    reply,
                    _permit: permit,
                },
            ))
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn reply(kafka: &Kafka, frame: Bytes) -> Reply {
    match dispatch::handle(kafka, frame).await {
        Ok(Some(bytes)) => Reply::Frame(bytes),
        Ok(None) => Reply::Silent,
        Err(e) => {
            tracing::warn!(%e, "closing kafka connection");
            Reply::Close
        }
    }
}

async fn ordered(mut writer: OwnedWriteHalf, mut replies: mpsc::Receiver<(u64, Slot)>) {
    let mut next = 0u64;
    let mut pending = BTreeMap::new();
    while let Some((sequence, slot)) = replies.recv().await {
        pending.insert(sequence, slot);
        while let Some(slot) = pending.remove(&next) {
            match slot.reply {
                Reply::Frame(frame) => {
                    if write_frame(&mut writer, &frame).await.is_err() {
                        return;
                    }
                }
                Reply::Silent => {}
                Reply::Close => return,
            }
            next += 1;
        }
    }
}

fn api_key(frame: &[u8]) -> Option<i16> {
    Some(i16::from_be_bytes([*frame.first()?, *frame.get(1)?]))
}

async fn read_frame(reader: &mut OwnedReadHalf, max: usize) -> Result<Option<Bytes>, Error> {
    let mut prefix = [0u8; LENGTH_PREFIX];
    match reader.read_exact(&mut prefix).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let size = i32::from_be_bytes(prefix);
    if size < 0 || size as usize > max {
        return Err(Error::Protocol(format!(
            "frame of {size} bytes (max {max})"
        )));
    }
    let mut body = BytesMut::zeroed(size as usize);
    reader.read_exact(&mut body).await?;

    Ok(Some(body.freeze()))
}

async fn write_frame(writer: &mut OwnedWriteHalf, body: &[u8]) -> Result<(), Error> {
    let size = i32::try_from(body.len())
        .map_err(|_| Error::Protocol(format!("response of {} bytes", body.len())))?;
    let mut frame = BytesMut::with_capacity(LENGTH_PREFIX + body.len());
    frame.extend_from_slice(&size.to_be_bytes());
    frame.extend_from_slice(body);
    writer.write_all(frame.chunk()).await?;
    writer.flush().await?;

    Ok(())
}
