use async_channel::{Receiver, Sender, TryRecvError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{MeshError, Result};

use super::{GenericMessage, MessageConsumer, MessageProducer};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsyncChannelOptions {
    #[serde(default = "init_channel")]
    #[serde(skip)]
    channel: (Sender<GenericMessage>, Receiver<GenericMessage>),
}

fn init_channel() -> (Sender<GenericMessage>, Receiver<GenericMessage>) {
    ::async_channel::unbounded()
}

pub struct AsyncChannelProducer {
    tx: async_channel::Sender<GenericMessage>,
}

pub fn initialize_producer(options: &AsyncChannelOptions) -> AsyncChannelProducer {
    AsyncChannelProducer {
        tx: options.channel.0.clone(),
    }
}

#[async_trait]
impl MessageProducer for AsyncChannelProducer {
    async fn send_message(&mut self, msg: &GenericMessage) -> Result<()> {
        self.tx
            .send(msg.clone())
            .await
            .map_err(|e| MeshError::Messaging(e.to_string()))?;
        Ok(())
    }
}

pub struct AsyncChannelConsumer {
    rx: async_channel::Receiver<GenericMessage>,
}

pub fn initialize_consumer(options: &AsyncChannelOptions) -> AsyncChannelConsumer {
    AsyncChannelConsumer {
        rx: options.channel.1.clone(),
    }
}

#[async_trait]
impl MessageConsumer for AsyncChannelConsumer {
    async fn receive_message(&mut self) -> Result<(u64, GenericMessage)> {
        let msg = self
            .rx
            .recv()
            .await
            .map_err(|e| MeshError::Messaging(e.to_string()))?;
        Ok((0, msg))
    }

    fn try_receive_message(&mut self) -> Result<(u64, GenericMessage)> {
        let msg = self.rx.try_recv().map_err(|e| match e {
            TryRecvError::Closed => MeshError::Messaging(e.to_string()),
            TryRecvError::Empty => MeshError::EmptyRecv(e.to_string()),
        })?;
        Ok((0, msg))
    }

    async fn ack_message(&mut self, _message_id: u64) -> Result<()> {
        Ok(())
    }
}
