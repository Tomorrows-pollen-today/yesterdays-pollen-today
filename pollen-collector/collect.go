package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Tomorrows-pollen-today/yesterdays-pollen-today/common/dataaccess"
	"github.com/mmcdole/gofeed"
)

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
		todaysPollen, err := getTodaysPollen("københavn", "græs")
		if err != nil {
			log.Println(err)
			return
		}

		dateForInsert := dataaccess.TimestampToDate(time.Now())

		data := &dataaccess.PollenSample{
			PollenCount: todaysPollen,
			Date:        dateForInsert,
		}
		log.Println(data)
		pollenRepo.UpsertPollenCount(data)
	}()

	waitGroup.Wait()
}

type azurePollenResponse struct {
	Results struct {
		PredictedPollenCount struct {
			Type  string `json:"type"`
			Value struct {
				ColumnNames []string   `json:"ColumnNames"`
				ColumnTypes []string   `json:"ColumnTypes"`
				Values      [][]string `json:"Values"`
			} `json:"value"`
		} `json:"predicted_pollen_count"`
	} `json:"Results"`
}

// PollenPrediction holds a parsed result from the prediction service
type PollenPrediction struct {
	PollenType           dataaccess.PollenType
	PredictedPollenCount float32
}

func parsePredictionValues(values [][]string) (*[]*PollenPrediction, error) {
	result := make([]*PollenPrediction, len(values))
	for i, value := range values {
		var err error
		result[i], err = parsePredictionValue(value)
		if err != nil {
			return &result, err
		}
	}
	return &result, nil
}

func parsePredictionValue(value []string) (*PollenPrediction, error) {
	prediction := &PollenPrediction{}
	switch value[0] {
	case "birch":
		prediction.PollenType = dataaccess.PollenTypeBirch
	case "grass":
		prediction.PollenType = dataaccess.PollenTypeGrass
	default:
		return nil, fmt.Errorf("Unknown pollen type: %s", value[0])
	}
	parsedFloat, err := strconv.ParseFloat(value[1], 32)
	if err != nil {
		return nil, err
	}
	prediction.PredictedPollenCount = float32(parsedFloat)
	return prediction, nil
}

func getTomorrowsPollen() (*[]*PollenPrediction, error) {
	client := &http.Client{}
	postBody, err := json.Marshal(map[string]interface{}{"GlobalParameters": map[string]string{
		"Output_name": "",
	}})
	if err != nil {
		panic(fmt.Errorf("json.Marshal: %v", err))
	}

	request, err := http.NewRequest("POST", config.PredictionAPIEndpoint, bytes.NewBuffer(postBody))
	if err != nil {
		panic(fmt.Errorf("NewRequest: %v", err))
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", config.PredictionAPIKey))
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")

	tomorrowsPollenResponse, err := client.Do(request)
	if err != nil {
		log.Fatal(err, tomorrowsPollenResponse)
		return nil, err
	}

	defer tomorrowsPollenResponse.Body.Close()
	data, _ := ioutil.ReadAll(tomorrowsPollenResponse.Body)

	var tomorrowsPollen azurePollenResponse
	err = json.Unmarshal(data, &tomorrowsPollen)
	if err != nil {
		log.Println(err, tomorrowsPollenResponse, tomorrowsPollenResponse.Body)
		return nil, err
	}

	return parsePredictionValues(tomorrowsPollen.Results.PredictedPollenCount.Value.Values)
}

func getTodaysPollen(city string, pollenType string) (int32, error) {
	fp := gofeed.NewParser()
	feed, err := fp.ParseURL("http://www.dmi.dk/vejr/services/pollen-rss/")

	if err != nil {
		log.Println(err, feed)
		return 0, err
	}
	for _, item := range feed.Items {
		if strings.ToLower(item.Title) == city {
			description := strings.ToLower(item.Description)
			description = strings.Replace(description, "\n", "", -1)
			description = strings.Replace(description, "\r", "", -1)
			description = strings.Replace(description, " ", "", -1)
			values := strings.Split(description, ";")
			for _, pollenDescription := range values {
				pollenKv := strings.Split(pollenDescription, ":")
				if pollenKv[0] == pollenType {
					value, err := strconv.ParseInt(pollenKv[1], 10, 32)
					if err != nil {
						log.Println(err, feed)
						return 0, err
					}
					return int32(value), err
				}
			}
		}
	}

	return 0, fmt.Errorf("Could not find pollen value for %v in %v", pollenType, city)
}

type azureHistoricalPollenResponse struct {
	Results struct {
		HistoricalPollenCount struct {
			Type  string `json:"type"`
			Value struct {
				ColumnNames []string   `json:"ColumnNames"`
				ColumnTypes []string   `json:"ColumnTypes"`
				Values      [][]string `json:"Values"`
			} `json:"value"`
		} `json:"historical_pollen_count"`
	} `json:"Results"`
}

func getHistoricalPollen() ([]*dataaccess.PollenSample, error) {
	client := &http.Client{}
	postBody, err := json.Marshal(map[string]interface{}{"GlobalParameters": map[string]string{}})
	if err != nil {
		panic(fmt.Errorf("json.Marshal: %v", err))
	}

	request, err := http.NewRequest("POST", config.HistoricalAPIEndpoint, bytes.NewBuffer(postBody))
	if err != nil {
		panic(fmt.Errorf("NewRequest: %v", err))
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %v", config.HistoricalAPIKey))
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json")

	historicalPollenResponse, err := client.Do(request)
	if err != nil {
		log.Fatal(err, historicalPollenResponse)
		return nil, err
	}
	defer historicalPollenResponse.Body.Close()
	data, _ := ioutil.ReadAll(historicalPollenResponse.Body)

	var historicalPollen azureHistoricalPollenResponse
	err = json.Unmarshal(data, &historicalPollen)
	if err != nil {
		log.Println(err, historicalPollenResponse, string(data))
		return nil, err
	}
	var result []*dataaccess.PollenSample
	var count = len(historicalPollen.Results.HistoricalPollenCount.Value.Values)
	for index := 0; index < count; index++ {
		var currentResult = historicalPollen.Results.HistoricalPollenCount.Value.Values[index]
		date, err := time.Parse("1/2/2006 15:04:05 AM", currentResult[0])
		if err != nil {
			log.Println(err, currentResult)
			continue
		}
		historicalPollenValue, err := strconv.ParseInt(currentResult[1], 10, 32)
		if err != nil {
			log.Println(err, currentResult)
			continue
		}
		historicalPredictedPollenValue, err := strconv.ParseFloat(currentResult[2], 32)
		if err != nil {
			log.Println(err, currentResult)
			continue
		}
		var pollenCount = int32(historicalPollenValue)
		var predictedPollenCount = float32(historicalPredictedPollenValue)
		var pollenSample = &dataaccess.PollenSample{
			Date:                 dataaccess.TimestampToDate(date),
			PollenCount:          pollenCount,
			PredictedPollenCount: predictedPollenCount,
		}
		result = append(result, pollenSample)
	}
	return result, nil

}
