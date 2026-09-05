package input

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
)

func ForEach(path string, agencyId int, batchSize int, process func([]bet.Bet) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var batch []bet.Bet
	for scanner.Scan() {
		b, err := parseBet(scanner.Text(), agencyId)
		if err != nil {
			return err
		}
		batch = append(batch, b)
		if len(batch) == batchSize {
			if err := process(batch); err != nil {
				return err
			}
			batch = batch[:0] // reutilizo el slice, no aloco de nuevo
		}
	}
	if len(batch) > 0 {
		if err := process(batch); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func parseBet(line string, agencyId int) (bet.Bet, error) {
	fields := strings.Split(line, ",")
	document, err := strconv.Atoi(fields[2])
	if err != nil {
		return bet.Bet{}, err
	}
	number, err := strconv.Atoi(fields[4])
	if err != nil {
		return bet.Bet{}, err
	}
	return bet.Bet{
		AgencyId:      agencyId,
		FirstName:     fields[0],
		LastName:      fields[1],
		DocumentNumber: document,
		Birthdate:     fields[3],
		Number:        number,
	}, nil
}