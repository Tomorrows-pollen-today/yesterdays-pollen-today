package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

// PollenDate holds a pollencount for a given date
type PollenDate struct {
	PollenCount int       `json:"pollencount"`
	Date        time.Time `json:"date"`
}

func main() {
	r := mux.NewRouter()
	apiRouter := r.PathPrefix("/api").Subrouter()
	apiRouter.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		data := PollenDate{
			PollenCount: 10,
			Date:        time.Now(),
		}

		json.NewEncoder(w).Encode(data)
	})

	http.ListenAndServe(":8080", r)
}
