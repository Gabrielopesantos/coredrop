//! The long-running daemon pod: it installs `core_pattern`, writes the
//! handler config, uploads the spool, and reports each capture as a log
//! line, a k8s Event and metrics.

pub mod health;
pub mod k8s_events;
pub mod metrics;
pub mod upload;
pub mod uploader;
