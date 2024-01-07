use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{
        BasicAckArguments, BasicConsumeArguments, BasicPublishArguments, BasicQosArguments,
        Channel, ConsumerMessage, QueueDeclareArguments,
    },
    connection::{Connection, OpenConnectionArguments},
    BasicProperties, DELIVERY_MODE_TRANSIENT,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{MeshError, Result};
use tokio::sync::mpsc::{self, UnboundedReceiver};

use super::{MessageConsumer, MessageProducer};

use super::GenericMessage;

pub struct RabbitMQConsumer {
    _conn: Connection,
    ch: Channel,
    rx: UnboundedReceiver<ConsumerMessage>,
    ctag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RabbitMQConnectionOptions {
    /// The url where the broker is hosted
    pub url: String,
    /// The port where the broker is hosted
    pub port: u16,
    /// The name or unique id of the queue
    pub queue_id: String,
    pub username: String,
    pub password: String,
}

async fn rabbitmq_connect(options: &RabbitMQConnectionOptions) -> Result<(Connection, Channel)> {
    let conn = Connection::open(&OpenConnectionArguments::new(
        &options.url,
        options.port,
        &options.username,
        &options.password,
    ))
    .await?;

    conn.register_callback(DefaultConnectionCallback).await?;

    let ch = conn.open_channel(None).await?;

    ch.register_callback(DefaultChannelCallback).await?;

    Ok((conn, ch))
}

async fn initialize_consumer(
    ch: &Channel,
    options: &RabbitMQConnectionOptions,
) -> Result<(String, mpsc::UnboundedReceiver<ConsumerMessage>)> {
    let q_args = QueueDeclareArguments::default()
        .queue(options.queue_id.clone())
        .durable(false)
        .finish();

    let (queue_name, _, _) = ch.queue_declare(q_args).await?.ok_or(MeshError::Messaging(
        "Failed to declare write queue!".into(),
    ))?;

    ch.basic_qos(BasicQosArguments::new(0, 1, true)).await?;

    let consumer_args = BasicConsumeArguments::new(&queue_name, "");

    Ok(ch.basic_consume_rx(consumer_args).await?)
}

impl RabbitMQConsumer {
    pub async fn initialize(options: &RabbitMQConnectionOptions) -> Result<Self> {
        let (_conn, ch) = rabbitmq_connect(options).await?;

        let (ctag, rx) = initialize_consumer(&ch, options).await?;

        Ok(Self {
            _conn,
            ch,
            rx,
            ctag,
        })
    }
}

#[async_trait]
impl MessageConsumer for RabbitMQConsumer {
    async fn receive_message(&mut self) -> Result<(u64, GenericMessage)> {
        let raw_msg = self.rx.recv().await.ok_or(MeshError::Messaging(format!(
            "Message consumer {} channel has been closed!",
            self.ctag
        )))?;

        let msg_id = raw_msg
            .deliver
            .ok_or(MeshError::Messaging(format!(
                "Message consumer {} received a message with no id!",
                self.ctag
            )))?
            .delivery_tag();

        let bytes = raw_msg
            .content
            .ok_or(MeshError::Messaging(format!(
                "Message consumer {} received a message with no content!",
                self.ctag
            )))
            .map_err(|e| MeshError::BadMessage((msg_id, e.to_string())))?;

        let msg: GenericMessage = serde_json::from_slice(&bytes)
            .map_err(|e| MeshError::BadMessage((msg_id, e.to_string())))?;

        Ok((msg_id, msg))
    }

    fn try_receive_message(&mut self) -> Result<(u64, GenericMessage)> {
        let raw_msg = self.rx.try_recv()?;

        let msg_id = raw_msg
            .deliver
            .ok_or(MeshError::Messaging(format!(
                "Message consumer {} received a message with no id!",
                self.ctag
            )))?
            .delivery_tag();

        let bytes = raw_msg.content.ok_or(MeshError::Messaging(format!(
            "Message consumer {} received a message with no content!",
            self.ctag
        )))?;

        let msg: GenericMessage = serde_json::from_slice(&bytes)?;

        Ok((msg_id, msg))
    }

    async fn ack_message(&mut self, message_id: u64) -> Result<()> {
        Ok(self
            .ch
            .basic_ack(BasicAckArguments::new(message_id, false))
            .await?)
    }
}

async fn initialize_producer(
    ch: &Channel,
    options: &RabbitMQConnectionOptions,
) -> Result<(BasicPublishArguments, BasicProperties)> {
    let q_args = QueueDeclareArguments::default()
        .queue(options.queue_id.clone())
        .durable(false)
        .finish();

    let (queue_name, _, _) = ch.queue_declare(q_args).await?.ok_or(MeshError::Messaging(
        "Failed to declare write queue!".into(),
    ))?;

    let pub_args = BasicPublishArguments::new("", &queue_name);
    let pub_props = BasicProperties::default()
        .with_delivery_mode(DELIVERY_MODE_TRANSIENT)
        .finish();

    Ok((pub_args, pub_props))
}

pub struct RabbitMQProducer {
    _conn: Connection,
    ch: Channel,
    pub_args: BasicPublishArguments,
    pub_props: BasicProperties,
}

impl RabbitMQProducer {
    pub async fn initialize(options: &RabbitMQConnectionOptions) -> Result<Self> {
        let (_conn, ch) = rabbitmq_connect(options).await?;

        let (pub_args, pub_props) = initialize_producer(&ch, options).await?;

        Ok(Self {
            _conn,
            ch,
            pub_args,
            pub_props,
        })
    }
}

#[async_trait]
impl MessageProducer for RabbitMQProducer {
    async fn send_message(&mut self, msg: &GenericMessage) -> Result<()> {
        let payload = serde_json::to_vec(msg)?;
        self.ch
            .basic_publish(self.pub_props.clone(), payload, self.pub_args.clone())
            .await?;
        Ok(())
    }
}
