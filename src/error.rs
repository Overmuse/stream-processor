use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Message received from Kafka with empty payload")]
    EmptyPayload,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Error from Kafka: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("Error from Serde: {source}\n{msg}")]
    Serde {
        #[source]
        source: serde_json::Error,
        msg: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
