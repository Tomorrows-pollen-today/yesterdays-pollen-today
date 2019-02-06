package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Tomorrows-pollen-today/yesterdays-pollen-today/common/dataaccess"
)

// HistoricalPollenCount holds pollen data scraped from astma-allergi.dk
type HistoricalPollenCount struct {
	Date        time.Time
	PollenCount int
}

type highchartSeries struct {
	Visible bool            `json:"visible"`
	Name    string          `json:"name"`
	Data    [][]interface{} `json:"data"`
}

// GetPollenData retrieves historical pollen data from astma-allergi.dk and parses them out to an array
func GetPollenData(pollenType dataaccess.PollenType, location *dataaccess.Location) ([]*HistoricalPollenCount, error) {
	var stationID, typeID int
	switch pollenType {
	case dataaccess.PollenTypeGrass:
		typeID = 28
		break
	case dataaccess.PollenTypeBirch:
		typeID = 7
		break
	default:
		panic("Unknown pollen type")
	}

	// TODO: this mapping should really not be maintained in code once we handle more than one location
	switch location.Location {
	case 0:
		stationID = 48
		break
	default:
		panic("Unknown location")
	}

	return getPollenData(stationID, typeID)
}

// getPollenData retrieves historical pollen data from astma-allergi.dk and parses them out to an array
func getPollenData(stationID int, typeID int) ([]*HistoricalPollenCount, error) {
	body, err := getPollenDataBody(stationID, typeID)
	if err != nil {
		return nil, err
	}
	return parsePollenDataBody(body)
}

func getPollenDataBody(stationID int, typeID int) (string, error) {
	client := http.DefaultClient

	values := url.Values{}
	values.Set("station_id", strconv.Itoa(stationID))
	values.Set("type_id", strconv.Itoa(typeID))

	request, err := http.NewRequest(http.MethodPost,
		"https://www.astma-allergi.dk/pollengrafer?p_p_id=graph_WAR_pollenportlet_INSTANCE_mt98szMFusmP&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&p_p_col_id=column-2&p_p_col_pos=2&p_p_col_count=4&_graph_WAR_pollenportlet_INSTANCE_mt98szMFusmP_action=graph",
		strings.NewReader(values.Encode()))
	if err != nil {
		log.Println(err, request)
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Origin", "https://www.astma-allergi.dk")
	request.Header.Set("Referer", "https://www.astma-allergi.dk/pollengrafer")
	request.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36")

	response, err := client.Do(request)
	if err != nil {
		log.Println(err, request)
		return "", err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Println(err, body)
		return "", err
	}
	return string(body), nil
}

func parsePollenDataBody(body string) ([]*HistoricalPollenCount, error) {
	jsonRegex := regexp.MustCompile(`series: (\[{visible:false,name:.*)`)
	dateRegex := regexp.MustCompile(`Date\.UTC\(1972,(\d+),(\d+)\)`)
	attributeRegex := regexp.MustCompile(`(visible|name|data):`)

	pollenJSON := jsonRegex.FindStringSubmatch(body)[1]
	pollenJSON = dateRegex.ReplaceAllString(pollenJSON, `"$0"`)
	pollenJSON = attributeRegex.ReplaceAllString(pollenJSON, `"$1":`)
	pollenJSON = strings.Replace(pollenJSON, `'`, `"`, -1)

	var pollenValues []highchartSeries
	err := json.Unmarshal([]byte(pollenJSON), &pollenValues)
	if err != nil {
		log.Println(err, pollenJSON)
		return nil, err
	}

	results := []*HistoricalPollenCount{}
	for _, pollenYear := range pollenValues {
		year, err := strconv.Atoi(pollenYear.Name)
		if err != nil {
			log.Println(err, pollenYear.Name)
			continue
		}
		for _, pollenDay := range pollenYear.Data {
			pollenCount := int(pollenDay[1].(float64))

			matches := dateRegex.FindStringSubmatch(pollenDay[0].(string))
			month, err := strconv.Atoi(matches[1])
			if err != nil {
				log.Println(err, matches[1])
				continue
			}
			day, err := strconv.Atoi(matches[2])
			if err != nil {
				log.Println(err, matches[2])
				continue
			}
			date := time.Date(year, time.Month(month+1), day, 0, 0, 0, 0, time.UTC)

			results = append(results, &HistoricalPollenCount{
				Date:        date,
				PollenCount: pollenCount,
			})
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Date.Before(results[j].Date)
	})
	return results, nil
}
