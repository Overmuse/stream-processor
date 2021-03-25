use crate::{
    error::{Error, Result},
    kafka::{consumer, producer},
    settings::KafkaSettings,
};
use futures::prelude::*;
use rdkafka::{message::Message, producer::FutureRecord};
use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;
use tracing::{debug, error, info, trace};

pub trait StreamProcessor {
    type Input: DeserializeOwned;
    type Output: Serialize + std::fmt::Debug;

    fn handle_message(&self, input: Self::Input) -> Result<Self::Output>;
    fn assign_topic(&self, output: &Self::Output) -> &str;
    fn assign_key(&self, output: &Self::Output) -> &str;
}

pub struct StreamRunner<T: StreamProcessor> {
    processor: T,
    settings: KafkaSettings,
}

impl<T: StreamProcessor> StreamRunner<T> {
    pub fn new(processor: T, settings: KafkaSettings) -> Self {
        Self {
            processor,
            settings,
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting stream processor");
        let consumer = consumer(&self.settings)?;
        let producer = producer(&self.settings)?;

        let msg_stream = consumer.stream().map(|x| -> Result<T::Output> {
            let owned = x?.detach();
            let payload = owned.payload().ok_or(Error::EmptyPayload)?;
            let deserialized: T::Input = serde_json::from_slice(payload)?;
            self.processor.handle_message(deserialized)
        });
        msg_stream
            .for_each_concurrent(None, |msg| async {
                if msg.is_err() {
                    error!("{:?}", msg);
                    return;
                }
                let msg = msg.expect("Guaranteed to be Ok");
                debug!("Message received: {:?}", msg);
                let serialized = serde_json::to_string(&msg).expect("Failed to serialize message");
                let topic = self.processor.assign_topic(&msg);
                let key = self.processor.assign_key(&msg);
                let record = FutureRecord::to(topic).key(key).payload(&serialized);
                let res = producer.send(record, Duration::from_secs(0)).await;
                match res {
                    Ok((partition, offset)) => trace!(
                        "Message successfully delivered to topic: {}, partition {}, offset {}",
                        topic,
                        partition,
                        offset
                    ),
                    Err((err, msg)) => error!("Message: {:?}\nError: {:?}", msg, err),
                }
            })
            .await;
        info!("Ending stream processor");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::{EnvFilter, FmtSubscriber};

    struct EchoProcessor;
    impl StreamProcessor for EchoProcessor {
        type Input = f32;
        type Output = f32;

        fn handle_message(&self, input: f32) -> Result<f32> {
            Ok(input)
        }
        fn assign_topic(&self, _output: &f32) -> &str {
            "topic"
        }
        fn assign_key(&self, _output: &f32) -> &str {
            "key"
        }
    }

    #[tokio::test]
    async fn it_works() {
        let subscriber = FmtSubscriber::builder()
            .with_env_filter(EnvFilter::from_default_env())
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();
        let settings = KafkaSettings::test_settings();
        let runner = StreamRunner::new(EchoProcessor, settings);
        runner.run().await.unwrap();
    }
}
