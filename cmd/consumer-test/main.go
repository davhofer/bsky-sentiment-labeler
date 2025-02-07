package main

import (
	"context"

	"bsky.watch/labeler/ingestion"
)



func main() {

    ctx := context.Background()
    ingestion.RunJetstreamConsumer(ctx, nil)

}
