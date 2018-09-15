package main

import (
	"bytes"
	"encoding/json"
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

	"github.com/mmcdole/gofeed"

	"github.com/Tomorrows-pollen-today/yesterdays-pollen-today/common/dataaccess"
)

var config *CollectorConfig

func main() {
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

		data := &dataaccess.PollenDate{
			PredictedPollenCount: &tomorrowsPollen,
			Date:                 dateForInsert,
		}
		log.Println(data)
		pollenRepo.UpsertPredictedPollenCount(data)
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

		data := &dataaccess.PollenDate{
			PollenCount: &todaysPollen,
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

func getTomorrowsPollen() (float32, error) {
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
		return 0, err
	} else {
		defer tomorrowsPollenResponse.Body.Close()
		data, _ := ioutil.ReadAll(tomorrowsPollenResponse.Body)

		var tomorrowsPollen azurePollenResponse
		err = json.Unmarshal(data, &tomorrowsPollen)
		if err != nil {
			log.Println(err, tomorrowsPollenResponse, tomorrowsPollenResponse.Body)
			return 0, err
		}
		tomorrowsPollenValue, err := strconv.ParseFloat(tomorrowsPollen.Results.PredictedPollenCount.Value.Values[0][0], 32)
		if err != nil {
			log.Println(err, tomorrowsPollenResponse)
			return 0, err
		}
		return float32(tomorrowsPollenValue), nil
	}
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
