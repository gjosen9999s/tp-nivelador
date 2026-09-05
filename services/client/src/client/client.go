package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/input"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 15
const CONNECTION_ATTEMPS_DELAY_MS = 500
const MESSAGE_LENGTH_BYTES = 4

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	defer client.conn.Close()

	agencyId, err := strconv.Atoi(client.config.AgencyId)
	if err != nil {
		logger.Error("parse-agency", logger.Fail, "err", err)
		return err
	}

	err = input.ForEach(client.config.InputFile, agencyId, 1, func(batch []bet.Bet) error {
		payload := protocol.Serialize(batch[0])
		return safe_socket.SendAll(client.conn, buildMessage(payload))
	})
	if err != nil {
		logger.Error("send-bets", logger.Fail, "err", err, "agency-id", agencyId)
		return err
	}

	tcpConn, ok := client.conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("connection is not TCP")
	}
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

	logger.Info("send-bets", logger.Success, "agency-id", agencyId, "winners-amount", len(winners))
	return nil
}

func buildMessage(payload []byte) []byte {
	message := make([]byte, MESSAGE_LENGTH_BYTES+len(payload))
	binary.BigEndian.PutUint32(message[:MESSAGE_LENGTH_BYTES], uint32(len(payload)))
	copy(message[MESSAGE_LENGTH_BYTES:], payload)
	return message
}

func (client *Client) readWinners() ([]bet.Bet, error) {
	var winners []bet.Bet
	for {
		header, err := safe_socket.RecvAll(client.conn, MESSAGE_LENGTH_BYTES)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		payloadLength := binary.BigEndian.Uint32(header)
		payload, err := safe_socket.RecvAll(client.conn, int(payloadLength))
		if err != nil {
			return nil, err
		}
		winners = append(winners, protocol.Deserialize(payload))
	}
	return winners, nil
}

func buildOutput(winners []bet.Bet) string {
	var sb strings.Builder
	for _, w := range winners {
		sb.WriteString(w.FirstName + "," + w.LastName + "," + fmt.Sprintf("%d", w.DocumentNumber) + "," + w.Birthdate + "," + fmt.Sprintf("%d", w.Number) + "\n")
	}
	return sb.String()
}