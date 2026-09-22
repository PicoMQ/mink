//! Turns a node address in grpc or grpc+tls form into the http or https URI a channel connects to.

pub const MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

pub fn http_uri(address: &str) -> String {
    match address.split_once("://") {
        Some(("grpc", rest)) => format!("http://{rest}"),
        Some(("grpc+tls", rest)) => format!("https://{rest}"),
        Some(_) => address.to_owned(),
        None => format!("http://{address}"),
    }
}
