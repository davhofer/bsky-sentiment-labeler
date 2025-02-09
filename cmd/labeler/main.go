package main

import (
	"context"
	"flag"
	"fmt"
    "time"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"

	"bsky.watch/utils/xrpcauth"

	"bsky.watch/labeler/account"
	"bsky.watch/labeler/config"
	"bsky.watch/labeler/ingestion"
	"bsky.watch/labeler/logging"
	"bsky.watch/labeler/server"
	"bsky.watch/labeler/simpleapi"
)

var (
	configFile  = flag.String("config", "config.yaml", "Path to the config file")
	listenAddr  = flag.String("listen-addr", ":8081", "IP:port to listen on")
	adminAddr   = flag.String("admin-addr", "", "IP:port to listen on with admin API")
	metricsAddr = flag.String("metrics-addr", "", "IP:port to export metrics on")
	logFile     = flag.String("log-file", "", "File to write the logs to. Will use stderr if not set")
	logFormat   = flag.String("log-format", "text", "Log entry format, 'text' or 'json'.")
	logLevel    = flag.Int("log-level", 1, "Log level. 0 - debug, 1 - info, 3 - error")
)

func runMain(ctx context.Context) error {
	log := zerolog.Ctx(ctx)

	b, err := os.ReadFile(*configFile)
	if err != nil {
		return fmt.Errorf("reading config file: %w", err)
	}

	config := &config.Config{}
	if err := yaml.Unmarshal(b, config); err != nil {
		return fmt.Errorf("parsing config file: %w", err)
	}
	server, err := server.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("instantiating a server: %w", err)
	}

    fmt.Println("Server created")

	server.SetAllowedLabels(config.LabelValues())

	if config.Password != "" && len(config.Labels.LabelValueDefinitions) > 0 {
		client := xrpcauth.NewClientWithTokenSource(ctx, xrpcauth.PasswordAuth(config.DID, config.Password))
		err := account.UpdateLabelDefs(ctx, client, &config.Labels)
		if err != nil {
			return fmt.Errorf("updating label definitions: %w", err)
		}
	}

    // TODO: improve frontend?
    fmt.Println("TODO: configure admin and metrics addresses")
	if *adminAddr != "" {
		frontend := simpleapi.New(server)
		mux := http.NewServeMux()
		mux.Handle("/label", frontend)

		go func() {
			if err := http.ListenAndServe(*adminAddr, mux); err != nil {
				log.Fatal().Err(err).Msgf("Failed to start listening on admin API address: %s", err)
			}
		}()
	}

	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		go func() {
			if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
				log.Fatal().Err(err).Msgf("Failed to start listening on metrics address: %s", err)
			}
		}()
	}

    fmt.Println("Starting jetstream consumer")

    // TODO: correct?
    go ingestion.RunJetstreamConsumer(ctx, server)

	mux := http.NewServeMux()
	mux.Handle("/xrpc/com.atproto.label.subscribeLabels", server.Subscribe())
	mux.Handle("/xrpc/com.atproto.label.queryLabels", server.Query())



    // TODO: how to handle teardown/exit of all systems gracefully?
    go ingestion.RunJetstreamConsumer(ctx, server)

	log.Info().Msgf("Starting HTTP listener...")
	return http.ListenAndServe(*listenAddr, mux)
}

func main() {
    fmt.Println("Main is running")
	flag.Parse()

    if *logFile == "" {
        formattedTime := time.Now().Format("02.01.2006-15.04")
        *logFile = "labeler-" + formattedTime + ".log"
    }

	ctx := logging.Setup(context.Background(), *logFile, *logFormat, zerolog.Level(*logLevel))
	log := zerolog.Ctx(ctx)

	if err := runMain(ctx); err != nil {
		log.Fatal().Err(err).Msgf("%s", err)
	}
}
