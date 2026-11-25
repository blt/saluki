//! Unix-specific helpers.
use std::{
    fs::Permissions,
    io,
    os::unix::fs::{FileTypeExt as _, PermissionsExt as _},
    path::Path,
};

use bytes::BufMut;
use socket2::SockRef;
use tokio::net::{UnixDatagram, UnixStream};
use tracing::{debug, trace};

#[cfg(target_os = "linux")]
mod ancillary;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use self::linux::{enable_uds_socket_credentials, socket_reuseport_supported, uds_sendmsg_with_creds};

#[cfg(not(target_os = "linux"))]
mod non_linux;
#[cfg(not(target_os = "linux"))]
pub use self::non_linux::{enable_uds_socket_credentials, socket_reuseport_supported};
use super::addr::{ConnectionAddress, ProcessCredentialsError, ProcessIdentity};

/// Ensures that the given path is read for use as a UNIX socket.
///
/// If the path already exists, and is a UNIX socket, it will be removed. If it's not a UNIX socket, an error will be
/// returned.
///
/// ## Errors
///
/// If the underlying system call fails, an error is returned.
pub async fn ensure_unix_socket_free<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref();

    // If the socket file already exists, we need to make sure it's already a UNIX socket, which means we're "clear" to
    // remove it before trying to claim it again when we create our listener.
    match tokio::fs::metadata(path).await {
        Ok(metadata) => {
            if !metadata.file_type().is_socket() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "path already exists and is not a Unix domain socket",
                ));
            }

            tokio::fs::remove_file(path).await?;

            trace!(
                socket_path = path.to_string_lossy().as_ref(),
                "Cleared existing Unix domain socket."
            );
        }
        Err(err) => {
            // If we can't find the file, that's good: nothing to clean up.
            //
            // Otherwise, forward the error.
            if err.kind() != io::ErrorKind::NotFound {
                return Err(err);
            }
        }
    }

    Ok(())
}

/// Sets the UNIX socket at the given path to be write-only by non-owners.
///
/// This ensures that normal clients can write to the socket but not read from it.
///
/// ## Errors
///
/// If the underlying system call fails, an error is returned.
pub async fn set_unix_socket_write_only<P: AsRef<Path>>(path: P) -> io::Result<()> {
    tokio::fs::set_permissions(path, Permissions::from_mode(0o722)).await
}

/// Returns the identity of the process on the other end of a connected Unix domain socket.
///
/// The kernel captures the peer's credentials when it connects (`SO_PEERCRED` on Linux), so they're fixed for the life of
/// the connection.
pub(crate) fn unix_peer_identity(socket: &UnixStream) -> ProcessIdentity {
    match socket.peer_cred() {
        Ok(creds) => match creds.pid() {
            Some(pid) => ProcessIdentity::new(pid, creds.uid(), creds.gid()),
            None => ProcessIdentity::Error(ProcessCredentialsError::UnsupportedPlatform),
        },
        Err(e) => {
            debug!(error = %e, "Failed to look up peer credentials for Unix domain socket connection.");
            ProcessIdentity::Error(ProcessCredentialsError::PeerLookupFailed)
        }
    }
}

/// Receives data from the Unix domain socket.
///
/// This function is specifically for Unix domain sockets in stream mode (that's, SOCK_STREAM), which are represented via
/// `UnixStream` in `tokio`.
///
/// On success, returns the number of bytes read and the address from whence the data came.
///
/// ## Errors
///
/// If the underlying system call fails, an error is returned.
pub async fn unix_recvmsg<B: BufMut>(socket: &mut UnixStream, buf: &mut B) -> io::Result<(usize, ConnectionAddress)> {
    unix_recvmsg_with_credential_check(socket, buf, || true).await
}

pub(crate) async fn unix_recvmsg_with_credential_check<B: BufMut>(
    socket: &UnixStream, buf: &mut B, mut needs_credentials: impl FnMut() -> bool,
) -> io::Result<(usize, ConnectionAddress)> {
    socket
        .async_io(tokio::io::Interest::READABLE, || {
            recvmsg_on_ready(socket, buf, &mut needs_credentials)
        })
        .await
}

/// Receives data from the Unix domain socket.
///
/// This function is specifically for Unix domain sockets in datagram mode (that's, SOCK_DGRAM), which are represented via
/// `UnixDatagram` in `tokio`.
///
/// On success, returns the number of bytes read and the address from whence the data came.
///
/// ## Errors
///
/// If the underlying system call fails, an error is returned.
pub async fn unixgram_recvmsg<B: BufMut>(socket: &UnixDatagram, buf: &mut B) -> io::Result<(usize, ConnectionAddress)> {
    unixgram_recvmsg_with_credential_check(socket, buf, || true).await
}

pub(crate) async fn unixgram_recvmsg_with_credential_check<B: BufMut>(
    socket: &UnixDatagram, buf: &mut B, mut needs_credentials: impl FnMut() -> bool,
) -> io::Result<(usize, ConnectionAddress)> {
    socket
        .async_io(tokio::io::Interest::READABLE, || {
            recvmsg_on_ready(socket, buf, &mut needs_credentials)
        })
        .await
}

fn recvmsg_on_ready<'sock, S, B: BufMut>(
    socket: &'sock S, buf: &mut B, _needs_credentials: &mut impl FnMut() -> bool,
) -> io::Result<(usize, ConnectionAddress)>
where
    SockRef<'sock>: From<&'sock S>,
{
    #[cfg(target_os = "linux")]
    let received = linux::uds_recvmsg(socket, buf, _needs_credentials());
    #[cfg(not(target_os = "linux"))]
    let received = non_linux::uds_recvmsg(socket, buf);
    received
}
