use flight_server_lib as flight;
use mesh::{conf::EnvConfigSettings, messaging::MessageBrokerOptions};
use query_runner_lib as query_runner;
use rest_server_lib as relay;
use std::{thread, time::Duration};
use tokio::runtime::Runtime;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let env_conf = EnvConfigSettings::init();

    // To use an in memory queue to communicate between threads, we must initialize
    // the queue now and pass clones to each thread. All other messaging types can
    // be initialized later within the threads (e.g. connecting to external rabbitMQ).
    let in_mem_messaging_opts = match &env_conf.msg_broker_opts {
        MessageBrokerOptions::AsyncChannel(_) => Some(env_conf.msg_broker_opts.clone()),
        #[cfg(feature = "rabbitmq")]
        _ => None,
    };
    let relay_in_mem_opts = in_mem_messaging_opts.clone();
    let relay_task = thread::spawn(move || {
        let rt = Runtime::new().expect("error creating runtime!");
        rt.block_on(async { relay::run(relay_in_mem_opts).await })
    });

    let flight_task = thread::spawn(|| {
        let rt = Runtime::new().expect("error creating runtime!");
        rt.block_on(async { flight::run().await })
    });

    let query_runner_in_mem_opts = in_mem_messaging_opts.clone();
    let query_runner_task = thread::spawn(move || {
        let rt = Runtime::new().expect("error creating runtime!");
        rt.block_on(async { query_runner::run(query_runner_in_mem_opts).await })
    });

    loop {
        if relay_task.is_finished() {
            match relay_task.join() {
                Ok(r) => match r {
                    Ok(_) => (),
                    Err(e) => panic!("Relay shut down with error {e}"),
                },
                Err(_e) => panic!("Relay shut down with unknown error"),
            }
            panic!("Relay shut down with no error!");
        } else if flight_task.is_finished() {
            match flight_task.join() {
                Ok(r) => match r {
                    Ok(_) => (),
                    Err(e) => panic!("Flight server shut down with error {e}"),
                },
                Err(_e) => panic!("Flight server shut down with unknown error"),
            }
            panic!("Flight server shut down with no error!");
        } else if query_runner_task.is_finished() {
            match query_runner_task.join() {
                Ok(r) => match r {
                    Ok(_) => (),
                    Err(e) => panic!("QueryRunner shut down with error {:?}", e),
                },
                Err(_e) => panic!("QueryRunner shut down with unknown error"),
            }
            panic!("QueryRunner shut down with no error!");
        }
        thread::sleep(Duration::from_secs(3));
    }
}
