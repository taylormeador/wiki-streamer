use eventsource_stream::Eventsource;
use futures_util::stream::StreamExt;
use reqwest::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // URL for Wikimedia's real-time recent changes stream
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";
    let client = Client::builder()
    .user_agent("wiki-streamer/0.1 (contact: taylor.r.meador@gmail.com)")
    .build()?;

    println!("Connecting to Wikipedia SSE stream...");

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
                println!("New Event Received:\n{}", event.data);
                println!("--------------------------------------------------\n");
            }
            Err(err) => {
                eprintln!("Error in stream: {}", err);
            }
        }
    }

    Ok(())
}

