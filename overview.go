package main

import (
	"context"

	"bsky.watch/labeler/account"
	"bsky.watch/labeler/config"
	"bsky.watch/labeler/logging"
	"bsky.watch/labeler/server"
	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/rs/zerolog"
)

func main() {
    ctx := context.Background()

    // logging 
    ctx = logging.Setup(ctx, "logfile.log", "setup string?", zerolog.InfoLevel)
    // TODO: how to use logger in context

    client := &xrpc.Client{}
    
    // account
	account.UpdateLabelDefs(ctx, client, nil)
    account.UpdateSigningKeyAndEndpoint(ctx, client, "my-token", "new-public-key", "endpoint")

    // config
    config := config.Config{}
    // only need to create the LabelValuedDefinitions, can update labels automatically
    config.UpdateLabelValues()
    config.LabelValues()

    // server
    sv, _ := server.NewWithConfig(ctx, &config)
    sv.Query()
    sv.IsEmpty()
    label := atproto.LabelDefs_Label{} // note: this is a specific label applied to a record. not a label definition
    sv.AddLabel(ctx, label) // this could be used to backfill the database...
    sv.Subscribe()
    sv.LabelEntries(ctx, "my-label")
    sv.SetAllowedLabels([]string{})
    sv.ImportEntries(nil)

}
