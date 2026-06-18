// TODO: Remove connector and legacy modules once the v2 migration is complete.
// The v2 connector is in the v2/ directory. The v2 migration tracker is at
// https://internal.example.com/migrations/v2-connector
pub mod ai;
pub mod config;
pub mod connector;
pub mod discovery;
pub mod health;
pub mod legacy;
pub mod messaging;
pub mod protocol;
pub mod registry;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const BUILD_PROFILE: &str = if cfg!(debug_assertions) {
    "debug"
} else {
    "release"
};
