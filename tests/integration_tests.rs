use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use rdkafka::{client::DefaultClientContext, ClientConfig};
use stream_processor::*;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

struct StreamDoubler;

impl StreamProcessor for StreamDoubler {
    type Input = f64;
    type Output = f64;

    fn handle_message(&self, input: Self::Input) -> Result<Self::Output, Error> {
        Ok(input * 2.0)
    }

    fn assign_topic(&self, _output: &Self::Output) -> &str {
        "test-output"
    }

    fn assign_key(&self, _output: &Self::Output) -> &str {
        "key"
    }
}

fn test_admin() -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .create()
        .unwrap()
}

fn test_producer() -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .create()
        .unwrap()
}

fn test_consumer() -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .set("group.id", "test-listener")
        .create()
        .unwrap()
}

#[tokio::test]
async fn main() {
    // Setup logger
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_env_filter(EnvFilter::from_default_env())
            .finish(),
    )
    .unwrap();
    // Create topics
    let admin_options = AdminOptions::new();
    let admin = test_admin();
    admin
        .create_topics(
            &[
                NewTopic::new("test-input", 1, TopicReplication::Fixed(1)),
                NewTopic::new("test-output", 1, TopicReplication::Fixed(1)),
            ],
            &admin_options,
        )
        .await
        .unwrap();

    // Setup stream processor
    tokio::spawn(async {
        let processor = StreamDoubler;
        let settings = KafkaSettings::new(
            "localhost:9094".into(),
            "test".into(),
            SecurityProtocol::Plaintext,
            vec!["test-input".into()],
        );
        let runner = StreamRunner::new(processor, settings);
        runner.run().await.unwrap();
    });

    // Setup test producer and consumer
    let test_producer = test_producer();
    let test_consumer = test_consumer();
    test_consumer.subscribe(&["test-output"]).unwrap();

    // Actual test
    test_producer
        .send_result(FutureRecord::to("test-input").key("test").payload("2"))
        .unwrap();
    let res = test_consumer.recv().await.unwrap().detach();
    let key = String::from_utf8(res.key().unwrap().into()).unwrap();
    let payload = String::from_utf8(res.payload().unwrap().into()).unwrap();
    assert_eq!(key, "key");
    assert_eq!(payload, "4.0");
}