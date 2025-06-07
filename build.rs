fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile_protos(
        &[
            "proto/wal.proto",
            "proto/message.proto",
            "proto/namespace.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}
