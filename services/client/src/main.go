package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	client "github.com/7574-sistemas-distribuidos/tp-nivelador/src/client"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

func loadConfig() (client.ClientConfig, error) {
	agencyID := os.Getenv("AGENCY_ID")
	if agencyID == "" {
		return client.ClientConfig{}, errors.New("AGENCY_ID environment variable is required")
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	inputFile := os.Getenv("INPUT_FILE")
	if inputFile == "" {
		return client.ClientConfig{}, errors.New("INPUT_FILE environment variable is required")
	}

	outputFile := os.Getenv("OUTPUT_FILE")
	if outputFile == "" {
		return client.ClientConfig{}, errors.New("OUTPUT_FILE environment variable is required")
	}

	batchSize := os.Getenv("BATCH_SIZE")
	if batchSize == "" {
		return client.ClientConfig{}, errors.New("BATCH_SIZE environment variable is required")
	}
	parsedBatchSize, err := strconv.Atoi(batchSize)
	if err != nil || parsedBatchSize <= 0 {
		return client.ClientConfig{}, errors.New("BATCH_SIZE must be a positive integer")
	}

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		AgencyID:   agencyID,
		InputFile:  inputFile,
		OutputFile: outputFile,
		BatchSize:  batchSize,
	}, nil
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	config, err := loadConfig()
	if err != nil {
		logger.Error("load-config", logger.Fail, "err", err)
		return 1
	}

	client, err := client.NewClient(ctx, config)
	if err != nil {
		if ctx.Err() != nil {
			logger.Info("connect-to-server", logger.Success, "cause", ctx.Err())
			return 0
		}
		logger.Error("client-new", logger.Fail, "err", err)
		return 1
	}

	go func() {
		<-ctx.Done()
		client.Close() // SIGTERM => cierro la conexion para desbloquear un RecvAll pendiente
	}()

	if err := client.Run(); err != nil {
		if ctx.Err() != nil {
			logger.Info("client-run", logger.Success, "cause", ctx.Err())
			return 0
		}
		logger.Error("client-run", logger.Fail, "err", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}
