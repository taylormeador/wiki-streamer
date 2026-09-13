use eventsource_stream::Eventsource;
use futures_util::stream::StreamExt;
use reqwest::Client;
use log::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    // URL for Wikimedia's real-time recent changes stream
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";
    let client = Client::builder()
    .user_agent("wiki-streamer/0.1 (contact: taylor.r.meador@gmail.com)")
    .build()?;

    info!("Connecting to Wikipedia SSE stream...");

    // Request the stream from the server
    let response = client
        .get(url)
        .send()
        .await?
        .error_for_status()?;

    // Convert the response body into an SSE stream
    let mut event_stream = response.bytes_stream().eventsource();

    // Iterate over incoming events asynchronously
    while let Some(event) = event_stream.next().await {
        match event {
            Ok(event) => {
                // event.data contains the JSON payload from Wikipedia
                info!("New Event Received:\n{}", event.data);
                info!("--------------------------------------------------\n");
            }
            Err(err) => {
                error!("Error in stream: {}", err);
            }
        }
    }

    Ok(())
}

