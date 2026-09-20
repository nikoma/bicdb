//! Reusable sync coordination for BicDB clients.
//!
//! This crate owns the app-independent sync loop. Desktop shells such as Tauri
//! should call this API from their background worker instead of reimplementing
//! BicDB bundle checkpointing in UI code.

mod checkpoint;
mod coordinator;
mod endpoint;
mod fs_endpoint;
mod lan;
mod mesh;

pub use checkpoint::{ClientSyncCheckpoint, SYNC_CHECKPOINT_FORMAT_VERSION};
pub use coordinator::{SyncCoordinator, SyncRunReport};
pub use endpoint::{PushBundleReport, SyncEndpoint};
pub use fs_endpoint::FileSyncEndpoint;
pub use lan::{
    sync_with_addr, LanBeacon, LanMesh, LanMeshConfig, LanPeer, DEFAULT_MULTICAST_ADDR,
    LAN_BEACON_MAGIC, LAN_BEACON_VERSION,
};
pub use mesh::{
    run_mesh_initiator, run_mesh_initiator_shared, run_mesh_responder, run_mesh_responder_shared,
    FrameTiming, MeshSyncReport, MAX_MESH_EVENTS_PER_BUNDLE, MAX_MESH_FRAME_BYTES,
    MESH_PROTOCOL_VERSION,
};
