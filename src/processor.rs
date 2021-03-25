use crate::{error::Result, intake::Intake, kafka::producer, settings::KafkaSettings};
use futures::StreamExt;
use rdkafka::producer::FutureRecord;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::time::Duration;
use tracing::{debug, error, info, trace};

pub trait StreamProcessor {
    type Input: DeserializeOwned;
    type Output: Serialize + std::fmt::Debug;

    fn handle_message(&self, input: Self::Input) -> Result<Self::Output>;
    fn assign_topic(&self, output: &Self::Output) -> &str;
    fn assign_key(&self, output: &Self::Output) -> &str;
}

pub struct StreamRunner<'a, I, T>
where
    I: Intake<'a, T::Input>,
    T: StreamProcessor,
{
    intake: &'a mut I,
    settings: KafkaSettings,
    phantom: PhantomData<T>,
}

impl<'a, I: Intake<'a, T::Input>, T: StreamProcessor> StreamRunner<'a, I, T> {
    pub fn new(intake: &'a mut I, settings: KafkaSettings) -> Self {
        Self {
            intake,
            settings,
            phantom: PhantomData,
        }
    }

    #[tracing::instrument(skip(self, processor, ctx))]
    pub async fn run(self, processor: T, ctx: I::InitializationContext) -> Result<()> {
        info!("Starting stream processor");
        let producer = producer(&self.settings)?;

        self.intake.initialize(ctx)?;

        self.intake
            .to_stream()
            .map(|msg| processor.handle_message(msg.unwrap()))
            .for_each_concurrent(None, |msg| async {
                if msg.is_err() {
                    error!("{:?}", msg);
                    return;
                }
                let msg = msg.expect("Guaranteed to be Ok");
                debug!("Message received: {:?}", msg);
                let serialized = serde_json::to_string(&msg).expect("Failed to serialize message");
                let topic = processor.assign_topic(&msg);
                let key = processor.assign_key(&msg);
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
