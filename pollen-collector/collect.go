package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Tomorrows-pollen-today/yesterdays-pollen-today/common/dataaccess"
)

type feedPollenType struct {
	FeedLocationName string
	FeedPollenName   string
	PollenType       dataaccess.PollenType
	Location         int
}

var config *CollectorConfig

func main() {
	fullHistory := flag.Bool("full-history", false, "Fetch historical data")
	flag.Parse()
	// Change directory to the same as executable
	executable, _ := os.Executable()
	exPath := filepath.Dir(executable)
	os.Chdir(exPath)

	config = getConfig()
	pollenRepo, err := dataaccess.GetConnection()
	if err != nil {
		panic("No db connection!")
	}
	defer pollenRepo.Close()

	pollenRepo.InitDb()

	if *fullHistory {
		log.Println("Collecting full history")
		historicalPollen, err := getHistoricalPollen()
		if err != nil {
			log.Println(err)
			return
		}
		var count = len(historicalPollen)
		log.Printf("Found %v historical pollenSamples", count)
		for index := 0; index < count; index++ {
			pollenRepo.UpsertPollenSample(historicalPollen[index])
		}
		return
	}

	var waitGroup sync.WaitGroup

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		tomorrowsPollen, err := getTomorrowsPollen()
		if err != nil {
			log.Println(err)
			return
		}

		dateForInsert := dataaccess.TimestampToDate(time.Now())
		dateForInsert = dateForInsert.AddDate(0, 0, 1)

		for _, pollenPrediction := range *tomorrowsPollen {
			data := &dataaccess.PollenSample{
				Date:                 dateForInsert,
				PollenType:           pollenPrediction.PollenType,
				Location:             dataaccess.Location{Location: 0},
				PredictedPollenCount: pollenPrediction.PredictedPollenCount,
			}
			pollenRepo.UpsertPredictedPollenCount(data)
		}
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		pollenTypes, err := pollenRepo.GetPollenTypes()
		if err != nil {
			log.Println(err)
			return
		}
		locations, err := pollenRepo.GetAllLocations()
		if err != nil {
			log.Println(err)
			return
		}
		for _, pollenType := range pollenTypes {
			for _, location := range locations {
				pollenData, err := GetPollenData(pollenType, location)
				if err != nil {
					log.Println(err)
					return
				}

				dateForInsert := dataaccess.TimestampToDate(time.Now())

				newestPollenData := pollenData[len(pollenData)-1]
				dist := dateForInsert.Sub(newestPollenData.Date)
				log.Printf("%s %s %s", newestPollenData.Date, dateForInsert, dist)
				if math.Abs(dist.Hours()) > 24 {
					err := fmt.Errorf("Newest pollen date from astma-allergi is not today newest:%v today:%v Dist:%v", newestPollenData.Date, dateForInsert, dist)
					log.Println(err)
					return
				}

				data := &dataaccess.PollenSample{
					Date:        dateForInsert,
					PollenType:  pollenType,
					Location:    dataaccess.Location{Location: location.Location},
					PollenCount: newestPollenData.PollenCount,
				}
				pollenRepo.UpsertPollenCount(data)
			}
		}
	}()

	waitGroup.Wait()
}
