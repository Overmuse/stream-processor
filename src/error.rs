use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Message received from Kafka with empty payload")]
    EmptyPayload,

    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),

    #[error("Error in config: {0:?}")]
    Config(#[from] config::ConfigError),

    #[error("Error from Kafka: {0:?}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("Error from Serde: {0:?}")]
    Serde(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
