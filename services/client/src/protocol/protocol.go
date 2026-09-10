// Formato del mensaje:
//
//	mensaje:  [4 bytes: largo del payload][payload]
//	batch:    [4: cantidad de apuestas][apuesta 1][apuesta 2]...
//	apuesta:  [4: agency_id][1: len(first)][first][1: len(last)][last]
//	          [4: document][1: len(birth)][birth][4: number]
//	ACK:      [4 bytes en 0] (mensaje sin payload)

package protocol

import (
	"encoding/binary"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/domain"
)

const (
	uint32Size         = 4
	lenSize            = 1
	MessageLengthBytes = uint32Size
)

func EncodeBet(item domain.Bet) ([]byte, error) {

	first := []byte(item.FirstName)
	last := []byte(item.LastName)
	birth := []byte(item.Birthdate)

	if len(first) > 255 || len(last) > 255 || len(birth) > 255 {
		return nil, errors.New("string field exceeds 255 bytes")
	}

	total := uint32Size + lenSize + len(first) + lenSize + len(last) +
		uint32Size + lenSize + len(birth) + uint32Size

	msg := make([]byte, total)
	off := 0

	binary.BigEndian.PutUint32(msg[off:], uint32(item.AgencyID))
	off += uint32Size

	msg[off] = byte(len(first))
	off += lenSize
	copy(msg[off:], first)
	off += len(first)

	msg[off] = byte(len(last))
	off += lenSize
	copy(msg[off:], last)
	off += len(last)

	binary.BigEndian.PutUint32(msg[off:], uint32(item.DocumentNumber))
	off += uint32Size

	msg[off] = byte(len(birth))
	off += lenSize
	copy(msg[off:], birth)
	off += len(birth)

	binary.BigEndian.PutUint32(msg[off:], uint32(item.Number))

	return msg, nil
}

func DecodeBet(payload []byte) (domain.Bet, error) {
	const errMsg = "malformed bet payload"
	bet := domain.Bet{}
	off := 0

	if len(payload) < uint32Size {
		return bet, errors.New(errMsg)
	}
	bet.AgencyID = int(binary.BigEndian.Uint32(payload[off:]))
	off += uint32Size

	if len(payload) < off+lenSize {
		return bet, errors.New(errMsg)
	}
	firstLen := int(payload[off])
	off += lenSize
	if len(payload) < off+firstLen {
		return bet, errors.New(errMsg)
	}
	bet.FirstName = string(payload[off : off+firstLen])
	off += firstLen

	if len(payload) < off+lenSize {
		return bet, errors.New(errMsg)
	}
	lastLen := int(payload[off])
	off += lenSize
	if len(payload) < off+lastLen {
		return bet, errors.New(errMsg)
	}
	bet.LastName = string(payload[off : off+lastLen])
	off += lastLen

	if len(payload) < off+uint32Size {
		return bet, errors.New(errMsg)
	}
	bet.DocumentNumber = int(binary.BigEndian.Uint32(payload[off:]))
	off += uint32Size

	if len(payload) < off+lenSize {
		return bet, errors.New(errMsg)
	}
	birthLen := int(payload[off])
	off += lenSize
	if len(payload) < off+birthLen {
		return bet, errors.New(errMsg)
	}
	bet.Birthdate = string(payload[off : off+birthLen])
	off += birthLen

	if len(payload) < off+uint32Size {
		return bet, errors.New(errMsg)
	}
	bet.Number = int(binary.BigEndian.Uint32(payload[off:]))

	return bet, nil
}

func EncodeBatch(bets []domain.Bet) ([]byte, error) {
	msg := make([]byte, uint32Size)
	binary.BigEndian.PutUint32(msg, uint32(len(bets)))

	for _, item := range bets {
		encoded, err := EncodeBet(item)
		if err != nil {
			return nil, err
		}
		msg = append(msg, encoded...)
	}
	return msg, nil
}

func EncodeMessage(payload []byte) []byte {
	message := make([]byte, uint32Size+len(payload))
	binary.BigEndian.PutUint32(message[:uint32Size], uint32(len(payload)))
	copy(message[uint32Size:], payload)
	return message
}

func DecodeLength(header []byte) uint32 {
	return binary.BigEndian.Uint32(header)
}

func IsAck(header []byte) bool {
	return DecodeLength(header) == 0
}
