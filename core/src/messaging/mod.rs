use crate::error::Result;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "rabbitmq")]
use self::rabbitmq::{RabbitMQConnectionOptions, RabbitMQConsumer, RabbitMQProducer};

#[cfg(feature = "async-channel")]
use self::in_memory::AsyncChannelOptions;

#[cfg(feature = "async-channel")]
pub mod in_memory;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;

#[derive(Debug, Clone, Serialize, Deserialize)]
/// A message about a [QueryTask][crate::model::query::QueryTask] to be executed. Full details about
/// the task are stored in the database.
pub struct QueryTaskMessage {
    pub id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Represents any possible message in the application.
pub enum GenericMessage {
    LocalQueryTask(QueryTaskMessage),
    RemoteQueryTask(QueryTaskMessage),
}

#[async_trait]
pub trait MessageConsumer: Send {
    /// Recieves a [GenericMessage] from producers suspending execution until available
    async fn receive_message(&mut self) -> Result<(u64, GenericMessage)>;

    /// Recieves a [GenericMessage] from producers immediately if available
    fn try_receive_message(&mut self) -> Result<(u64, GenericMessage)>;

    /// Marks a [GenericMessage] as processed
    async fn ack_message(&mut self, message_id: u64) -> Result<()>;
}

#[async_trait]
pub trait MessageProducer: Send {
    /// Sends a [GenericMessage] to consumers.
    async fn send_message(&mut self, msg: &GenericMessage) -> Result<()>;
}

/// Options used to initialize [MessageConsumer] and [MessageProducer]
/// trait objects.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum MessageBrokerOptions {
    #[cfg(feature = "rabbitmq")]
    /// Options to initialize a RabbitMQ [MessageConsumer] or [MessageProducer]
    RabbitMQ(RabbitMQConnectionOptions),
    #[cfg(feature = "async-channel")]
    /// Options to initialize an in memory AsyncChannel [MessageConsumer] or [MessageProducer]
    /// Can only be used in single binary deployment mode
    AsyncChannel(AsyncChannelOptions),
}

pub async fn initialize_producer(
    options: &MessageBrokerOptions,
) -> Result<Box<dyn MessageProducer>> {
    match options {
        #[cfg(feature = "rabbitmq")]
        MessageBrokerOptions::RabbitMQ(rabbitmq_opts) => {
            Ok(Box::new(RabbitMQProducer::initialize(rabbitmq_opts).await?))
        }
        #[cfg(feature = "async-channel")]
        MessageBrokerOptions::AsyncChannel(channel_opts) => {
            Ok(Box::new(in_memory::initialize_producer(channel_opts)))
        }
    }
}

pub async fn initialize_consumer(
    options: &MessageBrokerOptions,
) -> Result<Box<dyn MessageConsumer>> {
    match options {
        #[cfg(feature = "rabbitmq")]
        MessageBrokerOptions::RabbitMQ(rabbitmq_opts) => {
            Ok(Box::new(RabbitMQConsumer::initialize(rabbitmq_opts).await?))
        }
        #[cfg(feature = "async-channel")]
        MessageBrokerOptions::AsyncChannel(channel_opts) => {
            Ok(Box::new(in_memory::initialize_consumer(channel_opts)))
        }
    }
}
