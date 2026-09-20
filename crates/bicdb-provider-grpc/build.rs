fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/inventory.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/inventory.proto");
    Ok(())
}
