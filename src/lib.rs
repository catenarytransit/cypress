//! Cypress - A Rust-based geocoding system with ScyllaDB
//!
//! This library provides shared types and modules for the ingest and query binaries.

pub mod discord;
pub mod models;
pub mod pip;
pub mod scylla;
pub mod wikidata;

pub use models::{AdminLevel, Layer, OsmType, Place};
