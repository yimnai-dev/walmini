package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/yimnai-dev/walmini/internal/wal"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	w := wal.WAL{}

	w.Init(wal.WALConfig{})

	fmt.Println("Listening to both reads and writes...")

	for scanner.Scan() {
		input := scanner.Text()
		parts := strings.Fields(input)
		if len(parts) == 0 {
			fmt.Println("You cannot pass")
		}
		cmd := parts[0]
		switch cmd {
		case "--read":
			size := 5
			var delta int
			if len(parts) > 1 {
				nextPart := parts[1]
				if nextPart != "--offset" {
					cmdSize, err := strconv.Atoi(parts[1])
					if err != nil {
						log.Printf("You passed an invalid size: %s", err)
						continue
					}
					if cmdSize <= 0 {
						log.Printf("Read Size cannot be equal to or less than zero")
						continue
					}
					size = cmdSize
				}
				deltaIdx := slices.IndexFunc(parts, func(p string) bool {
					return p == "--offset"
				})
				if deltaIdx != -1 && deltaIdx+1 < len(parts) {
					deltaValue, err := strconv.Atoi(parts[deltaIdx+1])
					if err == nil {
						delta = deltaValue
					}

				}
			}
			records, err := w.ReadNext(size, delta)
			if err != nil {
				log.Printf("[WAL:READ]: Could not read records from the WAL: %s\n", err)
			}
			for _, record := range records {
				fmt.Println(record)
			}
		case "--write":
			err := w.Append(strings.Replace(input, "--write", "", 1))
			if err != nil {
				log.Printf("[WAL:WRITE]: Could not append a new record to the WAL: %s\n", err)
			}

		case "--help":
			fmt.Println("All messages must be prefixed by a --read or --write to denote what action you want to carry out.\n If you want to write to the WAL, start your message with teh `--write` prefix followed by the message e.g `--write hello-world`.\nTo denote the end of a `write`, make sure to terminate your message with a semicolumn. i.e --write hellow-wrodl;\nIf you want to read from our WAL, start your message with `--read` followed optionally by the size so --read on it's own will use the default size of 50 records.\nIf you need a specific size, simply do something like --read 20\n. To get help, simply send --help\nIf you want to seed the WAL with some sample records, simply run our --seed flag, optionally followed by\nthe number of records you want to seed.\n The maximum is 1000 so passing anything beyond that will cap it to 1000.")
			continue
		case "--seed":
			size := 1000
			if len(parts) > 1 {
				seedQty, err := strconv.Atoi(parts[1])
				if err != nil {
					log.Printf("You passed an invalid seed quantity: %v\n", seedQty)
					continue
				}
				if seedQty <= 0 {
					log.Printf("Seed Size must be greater than zero")
					continue
				}
				size = seedQty
			}
			errs := w.SeedWAL(size)
			if errs != nil && len(*errs) > 0 {
				for _, err := range *errs {
					log.Printf("[SEED:WAL]: Failed To Append Record to WAL: %s", err)
				}
			}
			
		default:
			fmt.Println("Your message is not formatted correctly. Please checkout our help center by sending --help as a message")
		}
	}
}
