package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", "https://stream.wikimedia.org/v2/stream/recentchange", nil)
	if err != nil {
		log.Fatalf("Failed to create request: %v", err)
	}

	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("User-Agent", "wiki-streamer/0.1 (contact: taylor.r.meador@gmail.com)")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Failed to connect to stream: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Unexpected status code: %d", resp.StatusCode)
	}

	fmt.Println("Connected to SSE stream. Waiting for events...")
}
