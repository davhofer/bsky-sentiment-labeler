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

// TODO: configure logging, read log directory from environment vars

const (
    jetstreamPerfLogIntervalSeconds = 5
)

const (
	serverAddr = "wss://jetstream.atproto.tools/subscribe"
)
// or use this?
// wss://jetstream2.us-east.bsky.network/subscribe

type PostEvent struct {
	Uri  string `json:"uri"`
	Text string `json:"text"`
}

var debugJetstreamOnly bool


// TODO: can i just run this as a goroutine? do i need to make sure it doesn't exit?
func RunJetstreamConsumer(ctx context.Context, s *server.Server) {
    // TODO: args from main func? context?

    debugJetstreamOnly = s == nil
    
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

    if !debugJetstreamOnly {
        // Process batches every 800 milliseconds
	    go processBatchQueue(ctx, s, 800 * time.Millisecond)
    }

	// cursor := time.Now().Add(30 * -time.Second).UnixMicro()

	// Every 5 seconds print the events read and bytes read and average event size
    // TODO: log this in some separate stats file

    formattedTime = time.Now().Format("02.01.2006-15.04")
    logfile = "jetstream-consumer-performance-" + formattedTime + ".log"

    perfLogFile, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatalf("Failed to open the specified log file %q: %s", logfile, err)
    }
    perfLogger := log.New(perfLogFile, "", log.LstdFlags|log.Lshortfile)
	go func() {
        defer perfLogFile.Close()
		ticker := time.NewTicker(time.Duration(jetstreamPerfLogIntervalSeconds) * time.Second)
        var eventsReadPrev int64 = 0
        var kilobytesReadPrev int64 = 0
		for {
			select {
			case <-ticker.C:
				eventsRead := c.EventsRead.Load()
				kilobytesRead := c.BytesRead.Load()/1000
                eventsDiff := eventsRead - eventsReadPrev
                kilobytesDiff := kilobytesRead - kilobytesReadPrev

                var avgEventSize int64
                if eventsDiff > 0 {
				    avgEventSize = (0100 * kilobytesDiff) / eventsDiff
                } else {
                    avgEventSize = 0 
                }
                eps := eventsDiff / int64(jetstreamPerfLogIntervalSeconds)
                KBps := kilobytesDiff / int64(jetstreamPerfLogIntervalSeconds)
                
                perfLogger.Println(time.Now().Local(), "| total events in last", jetstreamPerfLogIntervalSeconds, "sec:", eventsDiff, "| total KB in last", jetstreamPerfLogIntervalSeconds, "sec:", kilobytesDiff, "|", eps, "events/s |", KBps, "KB/s |", avgEventSize, "B avg event size")
                if debugJetstreamOnly {
                    fmt.Println(time.Now().Local(), "| total events in last", jetstreamPerfLogIntervalSeconds, "sec:", eventsDiff, "| total KB in last", jetstreamPerfLogIntervalSeconds, "sec:", kilobytesDiff, "|", eps, "events/s |", KBps, "KB/s |", avgEventSize, "B avg event size")
                }
                eventsReadPrev = eventsRead
                kilobytesReadPrev = kilobytesDiff
			}
		}
	}()

	if err := c.ConnectAndRead(ctx, nil/*&cursor*/); err != nil {
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
            
            if !debugJetstreamOnly {
                event := PostEvent{
                    Uri:  "at://" + event.Did + "/" + event.Commit.Collection + "/" + event.Commit.RKey,
                    Text: post.Text,
                }
                eventQueue <- event
            }
		}
	}

	return nil
}



type BatchRequest struct {
	Texts []string `json:"texts"`
}

type ClassifierResponse struct {
   Labels []string  `json:"labels"`
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
            Src: "did:plc:tlyhc6cyka6ehsests75oivf", // Src: DID of the account creating the labels
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
