package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/domain"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/input"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const connectionAttemptsMax = 15
const connectionAttemptsDelay = 500 * time.Millisecond

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyID   string
	InputFile  string
	OutputFile string
	BatchSize  string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(ctx context.Context, config ClientConfig) (*Client, error) {
	conn, err := connectToServer(ctx, config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(ctx context.Context, host, port string) (net.Conn, error) {
	const action = "connect-to-server"

	logger.Info(action, logger.InProgress)
	for i := range connectionAttemptsMax {
		if ctx.Err() != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			return nil, ctx.Err()
		}

		conn, err := net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			select {
			case <-time.After(connectionAttemptsDelay):
			case <-ctx.Done(): // si llega SIGTERM, se cancela la espera y se sale inmediatamente
				return nil, ctx.Err()
			}
			continue
		}

		logger.Info(action, logger.Success)
		return conn, nil
	}

	return nil, errors.New("could not connect to server")
}

func (client *Client) Run() error {
	defer client.conn.Close()

	agencyID, err := strconv.Atoi(client.config.AgencyID)
	if err != nil {
		logger.Error("parse-agency", logger.Fail, "err", err)
		return err
	}

	batchSize, err := strconv.Atoi(client.config.BatchSize)
	if err != nil {
		logger.Error("parse-batch-size", logger.Fail, "err", err)
		return err
	}

	err = input.ForEachBatch(client.config.InputFile, agencyID, batchSize, func(batch []domain.Bet) error {
		payload, err := protocol.EncodeBatch(batch)
		if err != nil {
			return err
		}
		message := protocol.EncodeMessage(payload)
		err = safe_socket.SendAll(client.conn, message)
		if err != nil {
			return err
		}

		// espera el ACK antes de enviar el siguiente batch => siempre un solo batch en vuelo
		ack, err := safe_socket.RecvAll(client.conn, protocol.MessageLengthBytes)
		if err != nil {
			return err
		}
		if !protocol.IsAck(ack) {
			return fmt.Errorf("unexpected message from server (expected ack)")
		}
		return nil
	})

	if err != nil {
		logger.Error("send-bets", logger.Fail, "err", err, "agency-id", agencyID)
		return err
	}

	//se cierro solo la mitad de escritura, se sigue pudiendo recibir los ganadores
	tcpConn := client.conn.(*net.TCPConn)
	if err := tcpConn.CloseWrite(); err != nil {
		logger.Error("close-write", logger.Fail, "err", err)
		return err
	}

	winners, err := client.readWinners()
	if err != nil {
		logger.Error("read-winners", logger.Fail, "err", err)
		return err
	}

	output := buildOutput(winners)
	if err := os.WriteFile(client.config.OutputFile, []byte(output), 0644); err != nil {
		logger.Error("write-output", logger.Fail, "err", err)
		return err
	}

	logger.Info("send-bets", logger.Success, "agency-id", agencyID, "winners-amount", len(winners))
	return nil
}

func (client *Client) readWinners() ([]domain.Bet, error) {
	var winners []domain.Bet
	for {
		header, err := safe_socket.RecvAll(client.conn, protocol.MessageLengthBytes)
		if err != nil {
			if errors.Is(err, io.EOF) { // el server termino de enviar ganadores
				break
			}
			return nil, err
		}
		payloadLength := protocol.DecodeLength(header)
		payload, err := safe_socket.RecvAll(client.conn, int(payloadLength))
		if err != nil {
			return nil, err
		}

		winner, err := protocol.DecodeBet(payload)
		if err != nil {
			return nil, err
		}
		winners = append(winners, winner)

	}
	return winners, nil
}

func (client *Client) Close() {
	if client.conn != nil {
		client.conn.Close()
	}
}

func buildOutput(winners []domain.Bet) string {
	var sb strings.Builder
	for _, w := range winners {
		sb.WriteString(fmt.Sprintf("%s,%s,%d,%s,%d\n", w.FirstName, w.LastName, w.DocumentNumber, w.Birthdate, w.Number))
	}
	return sb.String()
}
