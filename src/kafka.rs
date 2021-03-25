use crate::error::Result;
use crate::settings::KafkaSettings;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    producer::FutureProducer,
    ClientConfig,
};

pub fn consumer(settings: &KafkaSettings) -> Result<StreamConsumer> {
    let mut config = ClientConfig::new();
    let config = settings.config(&mut config);
    let consumer: StreamConsumer = config
        .set("group.id", &settings.group_id)
        .set("enable.ssl.certificate.verification", "false")
        .create()?;
    let subscription_topics: Vec<_> = settings.input_topics.iter().map(String::as_str).collect();
    consumer.subscribe(&subscription_topics)?;
    Ok(consumer)
}

pub fn producer(settings: &KafkaSettings) -> Result<FutureProducer> {
    let mut config = ClientConfig::new();
    let config = settings.config(&mut config);
    let producer: FutureProducer = config
        .set("enable.ssl.certificate.verification", "false")
        .create()?;
    Ok(producer)
}
