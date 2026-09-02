package parser

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/bet"
)

func ReadBets(path string) ([]bet.Bet, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close() 

	var bets []bet.Bet
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		bet, err := parseBet(scanner.Text())
		if err != nil {
			return nil, err
		}
		bets = append(bets, bet)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return bets, nil
}

func parseBet(line string) (bet.Bet, error) {
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
		FirstName: fields[0],
		LastName:  fields[1],
		DocumentNumber:  document,
		Birthdate: fields[3],
		Number:    number,
	}, nil
}