use crate::error::{Error, Result};
use futures::StreamExt;
use futures_core::Stream;
use rdkafka::{
    consumer::{Consumer, ConsumerContext, StreamConsumer},
    Message as KafkaMessage,
};
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{tungstenite::Message as TungsteniteMessage, WebSocketStream};

pub trait Intake<'a, T> {
    type InitializationContext;
    type Output: Stream<Item = Result<T>>;

    fn initialize(&self, ctx: Self::InitializationContext) -> Result<()>;
    fn to_stream(&'a mut self) -> Self::Output;
}

impl<'a, C: ConsumerContext + 'static, R: 'a, T: DeserializeOwned> Intake<'a, T>
    for StreamConsumer<C, R>
{
    type InitializationContext = Vec<String>;
    type Output = impl Stream<Item = Result<T>>;

    fn initialize(&self, ctx: Self::InitializationContext) -> Result<()> {
        let half_owned: Vec<_> = ctx.iter().map(String::as_str).collect();
        self.subscribe(&half_owned).map_err(From::from)
    }

    fn to_stream(&'a mut self) -> Self::Output {
        self.stream().map(|msg| -> Result<T> {
            let owned = msg?.detach();
            let payload = owned.payload().ok_or(Error::EmptyPayload)?;
            let deserialized: T = serde_json::from_slice(payload)?;
            Ok(deserialized)
        })
    }
}

impl<'a, S: AsyncRead + AsyncWrite + Unpin, T: DeserializeOwned> Intake<'a, T>
    for WebSocketStream<S>
{
    type InitializationContext = Vec<String>;
    type Output = impl Stream<Item = Result<T>>;

    fn initialize(&self, _ctx: Self::InitializationContext) -> Result<()> {
        Ok(())
    }

    fn to_stream(&'a mut self) -> Self::Output {
        self.filter_map(|msg| async {
            match msg.unwrap() {
                TungsteniteMessage::Text(txt) => {
                    Some(serde_json::from_str(&txt).map_err(From::from))
                }
                TungsteniteMessage::Binary(bytes) => {
                    Some(serde_json::from_slice(&bytes).map_err(From::from))
                }
                _ => None,
            }
        })
    }
}
