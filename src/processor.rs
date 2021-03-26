use crate::{
    error::{Error, Result},
    kafka::{consumer, producer},
    settings::KafkaSettings,
};
use futures::{
    future::{ready, Either},
    prelude::*,
};
use rdkafka::{message::Message, producer::FutureRecord};
use serde::{de::DeserializeOwned, Serialize};
use std::{borrow::Cow, time::Duration};
use tracing::{debug, error, info, trace};

#[async_trait::async_trait]
/// The common trait for all stream processors.
pub trait StreamProcessor {
    /// The input type to deserialize into from the Kafka input topics
    type Input: DeserializeOwned;
    /// The output type from the stream processor, which will be serialized and sent to Kafka.
    type Output: Serialize + std::fmt::Debug;

    /// Convert the input into a `impl Future<Result<Option<Vec<Self::Output>>>>`.  
    /// [`futures::Future`] because we might want to `await` in the implementation.  
    /// [`Result`] because our function might fail.  
    /// [`Option`] because we might not want to send any output. If this is `None`, we skip sending
    /// to Kafka.  
    /// [`Vec`] because we might want to send _many_ outputs for one input  
    async fn handle_message(&self, input: Self::Input) -> Result<Option<Vec<Self::Output>>>;
    /// Decide which topic to send the output to.
    fn assign_topic(&self, output: &Self::Output) -> Cow<str>;
    /// Decide which key to assign to the output.
    fn assign_key(&self, output: &Self::Output) -> Cow<str>;
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

    #[tracing::instrument(skip(self))]
    pub async fn run(&self) -> Result<()> {
        info!("Starting stream processor");
        let consumer = consumer(&self.settings)?;
        let producer = producer(&self.settings)?;

        let msg_stream = consumer
            .stream()
            .map(|x| -> Result<T::Input> {
                let owned = x?.detach();
                let payload = owned.payload().ok_or(Error::EmptyPayload)?;
                serde_json::from_slice(payload).map_err(From::from)
            })
            .filter_map(|msg| match msg {
                Ok(input) => {
                    let output = self
                        .processor
                        .handle_message(input)
                        .map(|msg| msg.transpose());
                    Either::Left(output)
                }
                Err(e) => {
                    error!("Error: {:?}", e);
                    Either::Right(ready(None))
                }
            });
        msg_stream
            .for_each_concurrent(None, |msgs| async {
                if msgs.is_err() {
                    error!("{:?}", msgs);
                    return;
                }
                let msgs = msgs.expect("Guaranteed to be Ok");
                for msg in msgs {
                    debug!("Message received: {:?}", msg);
                    let serialized =
                        serde_json::to_string(&msg).expect("Failed to serialize message");
                    let topic = self.processor.assign_topic(&msg);
                    let key = self.processor.assign_key(&msg);
                    let record = FutureRecord::to(topic.as_ref())
                        .key(key.as_ref())
                        .payload(&serialized);
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
                }
            })
            .await;
        info!("Ending stream processor");
        Ok(())
    }
}
