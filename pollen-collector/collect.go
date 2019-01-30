package main

import (
	"flag"
	"log"
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
			log.Println(data)
			pollenRepo.UpsertPredictedPollenCount(data)
		}
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		feed, err := getTodaysPollenFeed()
		if err != nil {
			log.Println(err)
			return
		}

		pollenTypesToExtract := []feedPollenType{
			feedPollenType{
				FeedLocationName: "københavn",
				FeedPollenName:   "græs",
				PollenType:       dataaccess.PollenTypeGrass,
				Location:         0,
			},
			feedPollenType{
				FeedLocationName: "københavn",
				FeedPollenName:   "birk",
				PollenType:       dataaccess.PollenTypeBirch,
				Location:         0,
			},
		}

		for _, feedPollenToExtract := range pollenTypesToExtract {
			todaysPollen, err := extractTodaysPollenFromFeed(feed,
				feedPollenToExtract.FeedLocationName, feedPollenToExtract.FeedPollenName)
			if err != nil {
				log.Println(err)
				return
			}

			dateForInsert := dataaccess.TimestampToDate(time.Now())

			data := &dataaccess.PollenSample{
				Date:        dateForInsert,
				PollenType:  feedPollenToExtract.PollenType,
				Location:    dataaccess.Location{Location: feedPollenToExtract.Location},
				PollenCount: todaysPollen,
			}
			log.Println(data)
			pollenRepo.UpsertPollenCount(data)
		}
	}()

	waitGroup.Wait()
}
