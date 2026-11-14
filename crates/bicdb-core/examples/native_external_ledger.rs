//! Independent, sequential qualification probe using the actual SDK.
//! This is neither a routing implementation nor a throughput benchmark.
#[cfg(feature = "tls-replication")]
#[path = "native_external_ledger/client.rs"]
mod client;

#[cfg(feature = "tls-replication")]
fn main() {
    match client::run() {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("incomplete experiment client: {error:?}");
            std::process::exit(2);
        }
    }
}

#[cfg(not(feature = "tls-replication"))]
fn main() {
    eprintln!("native_external_ledger requires tls-replication");
    std::process::exit(2);
}
