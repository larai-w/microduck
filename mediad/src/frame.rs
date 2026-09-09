//! A local `media.frame` endpoint for a recorder or perception process on the robot.
//!
//! A frame stays out of the WebRTC control channel: at the default geometry the UYVY payload is
//! about 1.8 MiB, so JSON/base64 would make a control request several MiB and let a slow peer tie
//! camera data to the network. This socket sends one JSON-RPC response header, then precisely
//! `bytes` raw bytes, which keeps the metadata inspectable without copying pixels through a text
//! encoding.
//!
//! **It asks for a frame rather than taking the last one.** [`Frames`] is a rendezvous, not a
//! cache: the capture branch copies a buffer only when a reader has asked for one
//! ([`crate::pipeline::Frames`] explains why — 1.84 MiB thirty times a second for readers that
//! want two). So a caller here waits for the capture that answers it, bounded by
//! [`pipeline::FRAME_TIMEOUT`], and a camera that has stopped is reported as a timeout rather than
//! answered with the frame it stopped on.
//!
//! The socket is group-readable like the other observation sockets: whoever may watch
//! `robot.state` may ask for a picture.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};
use duck_ipc_proto as proto;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::pipeline::Frames;

const SOCKET_MODE: u32 = 0o660;

/// The group that may ask for a frame. Deliberately the same one as `robotd`'s socket, `padd`'s
/// tap and `tof`'s stream: whoever may watch the robot may watch what it sees.
const GROUP: &str = "robot";

/// The longest request this endpoint will read. `media.frame` takes no parameters worth naming, so
/// anything approaching this is a client that has lost the plot.
const MAX_REQUEST_BYTES: usize = 4096;

#[derive(Debug, Serialize)]
struct Header {
    width: u32,
    height: u32,
    format: &'static str,
    bytes: usize,
    /// Wall time makes the snapshot joinable to a separately sampled robot state. It is not used
    /// to pace capture, so an NTP adjustment cannot affect the pipeline.
    captured_at_unix_us: u128,
}

/// Serve snapshots until the daemon exits.
pub async fn serve(socket: &Path, frames: Frames) -> Result<()> {
    if let Some(parent) = socket.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    // A stale socket from a daemon that did not shut down cleanly would refuse the bind. Removing
    // it is safe because only this unit ever owns this path.
    if socket.exists() {
        std::fs::remove_file(socket)
            .with_context(|| format!("removing stale {}", socket.display()))?;
    }
    let listener =
        UnixListener::bind(socket).with_context(|| format!("binding {}", socket.display()))?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(SOCKET_MODE))
        .with_context(|| format!("setting permissions on {}", socket.display()))?;
    if let Err(error) = give_to_group(socket, GROUP) {
        // Not fatal, and said out loud with what it means: the socket exists, and only `mediad`
        // and root can reach it. On a board that is a broken install; on a laptop it is a machine
        // with no `robot` group, which is ordinary.
        tracing::warn!(
            error = %error, group = GROUP, socket = %socket.display(),
            "media.frame stays private to mediad — nothing else can ask for a picture"
        );
    }
    tracing::info!(
        path = %socket.display(),
        mode = format!("{SOCKET_MODE:o}"),
        "serving media.frame locally"
    );

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let frames = frames.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle(stream, frames).await {
                        tracing::debug!(error = %error, "media.frame client ended");
                    }
                });
            }
            Err(error) => tracing::warn!(error = %error, "media.frame accept failed"),
        }
    }
}

async fn handle(stream: UnixStream, frames: Frames) -> Result<()> {
    let (read, mut write) = stream.into_split();
    // Bounded *before* the line is buffered. Checking the length afterwards would mean a client
    // could make this process hold an arbitrarily long line first, which is the thing the cap is
    // for. One byte over the cap is read so that "too large" stays distinguishable from a request
    // that exactly fills it.
    let mut reader = BufReader::new(read.take(MAX_REQUEST_BYTES as u64 + 1));
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    if line.len() > MAX_REQUEST_BYTES {
        write_response(
            &mut write,
            proto::Response::err(
                None,
                proto::Error::new(proto::code::INVALID_PARAMS, "request is too large"),
            ),
        )
        .await?;
        return Ok(());
    }
    let request: proto::Request = match serde_json::from_str(line.trim()) {
        Ok(request) => request,
        Err(error) => {
            write_response(
                &mut write,
                proto::Response::err(
                    None,
                    proto::Error::new(proto::code::PARSE_ERROR, error.to_string()),
                ),
            )
            .await?;
            return Ok(());
        }
    };
    if request.method != proto::method::MEDIA_FRAME {
        write_response(
            &mut write,
            proto::Response::err(
                request.id,
                proto::Error::new(
                    proto::code::METHOD_NOT_FOUND,
                    format!("{} is not served by mediad", request.method),
                ),
            ),
        )
        .await?;
        return Ok(());
    }
    // `next_frame` registers the demand and parks on a condvar until the capture that answers it
    // lands, so it cannot run on the runtime's thread.
    let frame = tokio::task::spawn_blocking(move || frames.next_frame()).await?;
    let Some(frame) = frame else {
        write_response(
            &mut write,
            proto::Response::err(
                request.id,
                proto::Error::new(
                    proto::code::INTERNAL_ERROR,
                    "no frame arrived within the capture timeout",
                ),
            ),
        )
        .await?;
        return Ok(());
    };
    let captured_at_unix_us = frame
        .captured_at
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    let header = Header {
        width: frame.width,
        height: frame.height,
        format: frame.format,
        bytes: frame.data.len(),
        captured_at_unix_us,
    };
    write_response(&mut write, proto::Response::ok(request.id, &header)).await?;
    write.write_all(&frame.data).await?;
    write.flush().await?;
    Ok(())
}

/// Hand the socket to `GROUP`. Mirrors `tof`'s stream and `padd`'s tap, including that a missing
/// group is a warning rather than a failure.
fn give_to_group(socket: &Path, group: &str) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(group).map_err(std::io::Error::other)?;
    // SAFETY: `getgrnam` reads the group database and returns a pointer into storage it owns. The
    // name is a valid C string for the length of the call, and nothing else in this process calls
    // into the group database.
    let entry = unsafe { libc::getgrnam(name.as_ptr()) };
    if entry.is_null() {
        return Err(std::io::Error::other(format!(
            "no {group} group on this system"
        )));
    }
    // SAFETY: checked non-null immediately above, and `struct group` is fully initialised by
    // `getgrnam` when it returns a pointer at all.
    let gid = unsafe { (*entry).gr_gid };

    let path = CString::new(socket.as_os_str().as_bytes()).map_err(std::io::Error::other)?;
    // SAFETY: a valid C string path; `-1` for the owner is the documented "leave it alone".
    if unsafe { libc::chown(path.as_ptr(), u32::MAX, gid) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

async fn write_response(
    write: &mut tokio::net::unix::OwnedWriteHalf,
    response: proto::Response,
) -> Result<()> {
    let mut line = serde_json::to_vec(&response)?;
    line.push(b'\n');
    write.write_all(&line).await?;
    write.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;
    use crate::pipeline::Frame;
    use tokio::io::AsyncReadExt;

    /// Stand in for the capture branch: wait for the demand this endpoint registers, then answer
    /// it once. Mirrors what `wire_frames` does on a buffer somebody asked for.
    fn answer_once(frames: Frames, frame: Frame) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !frames.take_request() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the endpoint never asked for a frame"
                );
                std::thread::yield_now();
            }
            frames.deliver(frame);
        })
    }

    async fn reply(frames: Frames, request: &str) -> proto::Response {
        let (mut client, server) = UnixStream::pair().unwrap();
        let task = tokio::spawn(handle(server, frames));
        client.write_all(request.as_bytes()).await.unwrap();
        client.shutdown().await.unwrap();
        let mut text = String::new();
        BufReader::new(client).read_line(&mut text).await.unwrap();
        task.await.unwrap().unwrap();
        serde_json::from_str(text.trim()).unwrap()
    }

    /// A camera that never delivers is a timeout, not a silent hang and not a stale frame.
    #[tokio::test]
    async fn a_capture_that_never_comes_is_an_explicit_error() {
        let response = reply(
            Frames::default(),
            "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"media.frame\",\"params\":{}}\n",
        )
        .await;
        assert_eq!(response.id, Some(proto::Id::Number(7)));
        assert_eq!(response.error.unwrap().code, proto::code::INTERNAL_ERROR);
    }

    /// Refused before any demand is registered, so an unknown method cannot make the capture
    /// branch copy 1.8 MiB for nothing.
    #[tokio::test]
    async fn an_unknown_method_is_refused_without_asking_for_a_frame() {
        let frames = Frames::default();
        let response = reply(
            frames.clone(),
            "{\"jsonrpc\":\"2.0\",\"id\":\"request\",\"method\":\"media.other\"}\n",
        )
        .await;
        assert_eq!(response.id, Some(proto::Id::Text("request".into())));
        assert_eq!(response.error.unwrap().code, proto::code::METHOD_NOT_FOUND);
        assert!(
            !frames.take_request(),
            "a refused method must not leave demand behind"
        );
    }

    #[tokio::test]
    async fn an_oversized_request_is_rejected_before_it_is_parsed() {
        let request = format!("{}\n", "x".repeat(MAX_REQUEST_BYTES + 1));
        let response = reply(Frames::default(), &request).await;
        assert_eq!(response.id, None);
        assert_eq!(response.error.unwrap().code, proto::code::INVALID_PARAMS);
    }

    /// The read is bounded before the line is buffered, so a client that never sends a newline
    /// cannot make this process hold an unbounded string.
    #[tokio::test]
    async fn a_request_without_a_newline_is_still_bounded() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let task = tokio::spawn(handle(server, Frames::default()));
        client
            .write_all("y".repeat(MAX_REQUEST_BYTES * 4).as_bytes())
            .await
            .unwrap();
        client.shutdown().await.unwrap();
        let mut text = String::new();
        BufReader::new(client).read_line(&mut text).await.unwrap();
        task.await.unwrap().unwrap();
        let response: proto::Response = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(response.error.unwrap().code, proto::code::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn a_frame_reply_names_and_follows_with_exactly_its_pixels() {
        let frames = Frames::default();
        let producer = answer_once(
            frames.clone(),
            Frame {
                width: 2,
                height: 1,
                format: "UYVY",
                captured_at: UNIX_EPOCH + Duration::from_secs(1),
                data: vec![128, 32, 128, 64],
            },
        );
        let (mut client, server) = UnixStream::pair().unwrap();
        let task = tokio::spawn(handle(server, frames));
        client
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"media.frame\"}\n")
            .await
            .unwrap();
        let mut reader = BufReader::new(client);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let response: proto::Response = serde_json::from_str(line.trim()).unwrap();
        let result = response.result.unwrap();
        assert_eq!(result["width"], 2);
        assert_eq!(result["height"], 1);
        assert_eq!(result["format"], "UYVY");
        assert_eq!(result["bytes"], 4);
        assert_eq!(result["captured_at_unix_us"], 1_000_000);
        let mut pixels = [0; 4];
        reader.read_exact(&mut pixels).await.unwrap();
        assert_eq!(pixels, [128, 32, 128, 64]);
        task.await.unwrap().unwrap();
        producer.join().unwrap();
    }
}
