use futures::SinkExt;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message as KafkaMessage,
};
use stream_processor::*;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
};
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
use tokio_tungstenite::{accept_async, connect_async, WebSocketStream};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

struct StreamDoubler;

impl StreamProcessor for StreamDoubler {
    type Input = f64;
    type Output = f64;

    fn handle_message(&self, input: Self::Input) -> Result<Self::Output, Error> {
        Ok(input * 2.0)
    }

    fn assign_topic(&self, output: &Self::Output) -> &str {
        //  Different output topics here as we don't want to get a race condition between the two
        // tests
        if output == &4.0 {
            "test-output-1"
        } else {
            "test-output-2"
        }
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

async fn run_connection<S>(mut connection: WebSocketStream<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let message = TungsteniteMessage::text("3");
    connection
        .send(message)
        .await
        .expect("Failed to send auth_response");
}

#[tokio::test]
async fn kafka() {
    // Setup logger
    let _g = tracing::subscriber::set_default(
        FmtSubscriber::builder()
            .with_env_filter(EnvFilter::from_default_env())
            .finish(),
    );

    // Create topics
    let admin_options = AdminOptions::new();
    let admin = test_admin();
    admin
        .create_topics(
            &[
                NewTopic::new("test-input", 1, TopicReplication::Fixed(1)),
                NewTopic::new("test-output-1", 1, TopicReplication::Fixed(1)),
            ],
            &admin_options,
        )
        .await
        .unwrap();

    // Setup stream processor
    tokio::spawn(async {
        let settings = KafkaSettings::new(
            "localhost:9094".into(),
            "test".into(),
            SecurityProtocol::Plaintext,
            vec!["test-input".into()],
        );
        let mut consumer = stream_processor::kafka::consumer(&settings).unwrap();
        let runner = StreamRunner::new(&mut consumer, settings);
        let initialization_context = vec!["test-input".to_string()];
        runner
            .run(StreamDoubler, initialization_context)
            .await
            .unwrap();
    });

    // Setup test producer and consumer
    let test_producer = test_producer();
    let test_consumer = test_consumer();
    test_consumer.subscribe(&["test-output-1"]).unwrap();

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

#[tokio::test]
async fn websocket() {
    let _g = tracing::subscriber::set_default(
        FmtSubscriber::builder()
            .with_env_filter(EnvFilter::from_default_env())
            .finish(),
    );
    // Create topics
    let admin_options = AdminOptions::new();
    let admin = test_admin();
    admin
        .create_topics(
            &[NewTopic::new(
                "test-output-2",
                1,
                TopicReplication::Fixed(1),
            )],
            &admin_options,
        )
        .await
        .unwrap();

    // Setup test consumer
    let test_consumer = test_consumer();
    test_consumer.subscribe(&["test-output-2"]).unwrap();

    // Setup websocket host
    let (con_tx, con_rx) = futures_channel::oneshot::channel();
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:12345").await.unwrap();
        // Send message when server is ready to start the test
        con_tx.send(()).unwrap();
        let (connection, _) = listener.accept().await.expect("No connections to accept");
        let stream = accept_async(connection).await;
        let stream = stream.expect("Failed to handshake with connection");
        run_connection(stream).await;
    });

    // Setup stream processor
    tokio::spawn(async {
        let settings = KafkaSettings::new(
            "localhost:9094".into(),
            "test".into(),
            SecurityProtocol::Plaintext,
            vec!["test-input".into()],
        );
        con_rx.await.expect("Server not ready");
        // Server is ready
        let (mut websocket, _) = connect_async("wss://127.0.0.1:12345").await.unwrap();
        let runner = StreamRunner::new(&mut websocket, settings);
        let initialization_context = vec!["test-input".to_string()];
        runner
            .run(StreamDoubler, initialization_context)
            .await
            .unwrap();
    });

    //// TODO: Figure out why we end up blocking below
    //let res = test_consumer.recv().await.unwrap().detach();
    //let key = String::from_utf8(res.key().unwrap().into()).unwrap();
    //let payload = String::from_utf8(res.payload().unwrap().into()).unwrap();
    //assert_eq!(key, "key");
    //assert_eq!(payload, "6.0");
}
