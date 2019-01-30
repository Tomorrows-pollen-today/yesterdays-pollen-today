package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/mmcdole/gofeed"
)

func getTodaysPollenFeed() (*gofeed.Feed, error) {
	fp := gofeed.NewParser()
	feed, err := fp.ParseURL("http://www.dmi.dk/vejr/services/pollen-rss/")

	if err != nil {
		log.Println(err, feed)
		return nil, err
	}

	return feed, nil
}

func extractTodaysPollenFromFeed(feed *gofeed.Feed, city string, pollenType string) (int, error) {

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
					if pollenKv[1] == "-" {
						return 0, nil
					}
					value, err := strconv.Atoi(pollenKv[1])
					if err != nil {
						log.Println(err, feed)
						return 0, err
					}
					return value, err
				}
			}
		}
	}

	return 0, fmt.Errorf("Could not find pollen value for %v in %v", pollenType, city)
}
