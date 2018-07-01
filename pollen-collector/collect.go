package main

import (
	"time"

	"github.com/Tomorrows-pollen-today/yesterdays-pollen-today/common/dataaccess"
)

func main() {
	pollenRepo, err := dataaccess.GetConnection()
	if err != nil {
		panic("No db connection!")
	}

	pollenRepo.InitDb()
	now := time.Now()
	data := &dataaccess.PollenDate{
		PollenCount: 10,
		Date:        time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC),
	}

	pollenRepo.UpsertPollen(data)
}
