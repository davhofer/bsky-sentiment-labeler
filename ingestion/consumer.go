package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"bsky.watch/labeler/server"
	"github.com/bluesky-social/indigo/api/atproto"
	apibsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/sequential"
	"github.com/bluesky-social/jetstream/pkg/models"
)

// the consumer subscribes to the jetstream, ingests all records, labels them, and adds them to the database
// idea: use a worker pool to process incoming events and label them?
// bottleneck sentiment classifier?

const (
	serverAddr = "wss://jetstream.atproto.tools/subscribe"
)
// or use this?
// wss://jetstream2.us-east.bsky.network/subscribe

type PostEvent struct {
	Uri  string `json:"uri"`
	Text string `json:"text"`
}

// TODO: can i just run this as a goroutine? do i need to make sure it doesn't exit?
func RunJetstreamConsumer(ctx context.Context, s *server.Server) {
    // TODO: args from main func? context?
    
    formattedTime := time.Now().Format("02.01.2006-15.04")
    logfile := "jetstream-consumer-" + formattedTime + ".log"

    logFile, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatalf("Failed to open the specified log file %q: %s", logfile, err)
    }

	slog.SetDefault(slog.New(slog.NewTextHandler(logFile, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	})))
	logger := slog.Default()

	config := client.DefaultClientConfig()
	config.WebsocketURL = serverAddr
	config.Compress = true
    config.WantedCollections = []string{"app.bsky.feed.post"}

	h := &handler{
		seenSeqs: make(map[int64]struct{}),
	}

	scheduler := sequential.NewScheduler("sentiment-labeler-ingestion", logger, h.HandleEvent)

	c, err := client.NewClient(config, logger, scheduler)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

    // Process batches every 800 milliseconds
	go processBatchQueue(ctx, s, 800 * time.Millisecond)

	cursor := time.Now().Add(30 * -time.Second).UnixMicro()

	// Every 5 seconds print the events read and bytes read and average event size
    // TODO: log this in some separate stats file

    formattedTime = time.Now().Format("02.01.2006-15.04")
    logfile = "jetstream-consumer-performance-" + formattedTime + ".log"

    perfLogFile, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatalf("Failed to open the specified log file %q: %s", logfile, err)
    }
    perfLogger := log.New(perfLogFile, "", log.LstdFlags|log.Lshortfile)
    logIntervalSeconds := 30*60 
	go func() {
        defer perfLogFile.Close()
		ticker := time.NewTicker(time.Duration(logIntervalSeconds) * time.Second)
        var eventsReadPrev int64 = 0
        var bytesReadPrev int64 = 0
		for {
			select {
			case <-ticker.C:
				eventsRead := c.EventsRead.Load()
				bytesRead := c.BytesRead.Load()
                eventsDiff := eventsRead - eventsReadPrev
                bytesDiff := bytesRead - bytesReadPrev

				avgEventSize := bytesDiff / eventsDiff
                eps := eventsDiff / int64(logIntervalSeconds)
                mbps := bytesDiff / (int64(logIntervalSeconds) * 1000)
                
                perfLogger.Println(time.Now().Local(), "| total events in last 30min:", eventsRead, "| total MB in last 30min:", bytesDiff/1000, "|", eps, "events/s |", mbps, "MB/s |", avgEventSize, "avg event size")
                eventsReadPrev = eventsRead
                bytesReadPrev = bytesRead
			}
		}
	}()

	if err := c.ConnectAndRead(ctx, &cursor); err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	slog.Info("shutdown")
}

func main() {
    ctx := context.Background()
    RunJetstreamConsumer(ctx, nil)
}

type handler struct {
	seenSeqs  map[int64]struct{}
	highwater int64
}

func (h *handler) HandleEvent(ctx context.Context, event *models.Event) error {
	// Unmarshal the record if there is one
    // TODO: any optimizations here?
	if event.Commit != nil && (event.Commit.Operation == models.CommitOperationCreate || event.Commit.Operation == models.CommitOperationUpdate) {
		switch event.Commit.Collection {
		case "app.bsky.feed.post":
			var post apibsky.FeedPost
			if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
				return fmt.Errorf("failed to unmarshal post: %w", err)
			}
            
            event := PostEvent{
                Uri:  "at://" + event.Did + "/" + event.Commit.Collection + "/" + event.Commit.RKey,
                Text: post.Text,
            }
            eventQueue <- event
		}
	}

	return nil
}



type BatchRequest struct {
	Texts []string `json:"texts"`
}

type ClassifierResponse struct {
   Labels []string  `json:"labels"`
   Scores []float64 `json:"scores"` 
}


// Channel for incoming events
var eventQueue = make(chan PostEvent, 500) // Buffered channel


// processBatchQueue collects events and sends them in batches
func processBatchQueue(ctx context.Context, s *server.Server, interval time.Duration) {
	ticker := time.NewTicker(interval) // Process every 0.5s
	defer ticker.Stop()

	for range ticker.C {
		processBatch(ctx, s)
	}
}



// processBatch collects queued events and sends them to the API
func processBatch(ctx context.Context, s *server.Server) {
	var batch []PostEvent

	// Collect all available events from the queue
	for {
		select {
		case event := <-eventQueue:
			batch = append(batch, event)
		default:
			// No more items in queue
			break
		}
	}

	// If there are no events, return early
	if len(batch) == 0 {
		return
	}

	// Extract texts for API request
	var texts []string
	for _, event := range batch {
		texts = append(texts, event.Text)
	}

	// Prepare JSON request
	requestBody, _ := json.Marshal(BatchRequest{Texts: texts})

	// Send batch request to API
	resp, err := http.Post("http://localhost:7080/predict/", "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		fmt.Println("Error sending batch request:", err)
		return
	}
	defer resp.Body.Close()

    var classifResp ClassifierResponse
    err = json.NewDecoder(resp.Body).Decode(&classifResp)
    if err != nil {
       // Handle error
		fmt.Println("Error parsing classifier response:", err)
		return
    }

    // TODO: use their mapping from scores to labels, or create our own?
    for i, labelText := range classifResp.Labels {
        labelVal := strings.ReplaceAll(strings.ToLower(labelText), " ", "-")
        label := atproto.LabelDefs_Label{
            Cts: time.Now().UTC().Format(time.RFC3339),
            Src: "did:plc:tlyhc6cyka6ehsests75oivf", 
            Uri: batch[i].Uri,
            Val: labelVal,
        }
        // TODO: write to DB
        if s != nil {
            s.AddLabel(ctx, label) 
        }
    }

    fmt.Println("classifications:", classifResp.Labels)


}
