#![feature(type_alias_impl_trait)]
pub mod error;
pub mod intake;
pub mod kafka;
pub mod processor;
pub mod settings;

pub use error::Error;
pub use processor::{StreamProcessor, StreamRunner};
pub use settings::{KafkaSettings, SecurityProtocol};
