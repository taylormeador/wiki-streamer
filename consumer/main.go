package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
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

	readStream(resp.Body)
}

func readStream(body io.Reader) {
	scanner := bufio.NewScanner(body)
	var buffer bytes.Buffer

	for scanner.Scan() {
		line := scanner.Text()

		// An empty line signals the end of a single SSE event block
		if line == "" {
			if buffer.Len() > 0 {
				processEvent(buffer.String())
				buffer.Reset()
			}
			continue
		}

		// Look for the "data:" prefix and load the contents into the buffer
		if after, ok := strings.CutPrefix(line, "data:"); ok {
			// handle multiple lines
			if buffer.Len() > 0 {
				buffer.WriteString("\n")
			}

			payload := strings.TrimSpace(after)
			buffer.WriteString(payload)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Stream read error: %v", err)
	}
}

func processEvent(data string) {
	fmt.Printf("New Event Received:\n%s\n\n", data)
}
