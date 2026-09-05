package protocol

import (
	"encoding/binary"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
)

const (
	uint32Size = 4
	lenSize    = 1
)

func Serialize(item bet.Bet) []byte {
	first := []byte(item.FirstName)
	last := []byte(item.LastName)
	birth := []byte(item.Birthdate)

	total := uint32Size + lenSize + len(first) + lenSize + len(last) +
		uint32Size + lenSize + len(birth) + uint32Size

	msg := make([]byte, total)
	off := 0

	binary.BigEndian.PutUint32(msg[off:], uint32(item.AgencyId))
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

	return msg
}

func Deserialize(payload []byte) bet.Bet {
	off := 0

	agencyId := int(binary.BigEndian.Uint32(payload[off:]))
	off += uint32Size

	firstLen := int(payload[off])
	off += lenSize
	first := string(payload[off : off+firstLen])
	off += firstLen

	lastLen := int(payload[off])
	off += lenSize
	last := string(payload[off : off+lastLen])
	off += lastLen

	document := int(binary.BigEndian.Uint32(payload[off:]))
	off += uint32Size

	birthLen := int(payload[off])
	off += lenSize
	birth := string(payload[off : off+birthLen])
	off += birthLen

	number := int(binary.BigEndian.Uint32(payload[off:]))

	return bet.Bet{
		AgencyId:       agencyId,
		FirstName:      first,
		LastName:       last,
		DocumentNumber: document,
		Birthdate:      birth,
		Number:         number,
	}
}