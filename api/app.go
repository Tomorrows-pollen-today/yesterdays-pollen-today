package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/Tomorrows-pollen-today/yesterdays-pollen-today/common/dataaccess"
	"github.com/gorilla/mux"
)

// PollenSampleDto holds a pollencount for a given date
type PollenSampleDto struct {
	PollenCount          int       `json:"pollencount"`
	PredictedPollenCount int       `json:"predictedpollencount"`
	Date                 time.Time `json:"date"`
}

type httpContext struct {
	Repo *dataaccess.PollenRepository
}

func main() {
	// Change directory to the same as executable
	executable, _ := os.Executable()
	exPath := filepath.Dir(executable)
	os.Chdir(exPath)

	repo, err := dataaccess.GetConnection()
	if err != nil {
		panic("Cannot connect to db")
	}
	repo.InitDb()
	context := &httpContext{
		Repo: repo,
	}

	router := mux.NewRouter()
	apiRouter := router.PathPrefix("/api").Subrouter()
	apiRouter.HandleFunc("/pollen/{date}", context.getPollen)
	apiRouter.HandleFunc("/pollen", context.getPollenRange).Queries("from", "{from}", "to", "{to}")

	http.ListenAndServe(":8001", router)
}

func writeObject(responseWriter http.ResponseWriter, output *json.Encoder, object interface{}, err error) {
	if err != nil {
		responseWriter.WriteHeader(http.StatusInternalServerError)
		output.Encode(err)
		return
	}
	if object == nil {
		responseWriter.WriteHeader(http.StatusNotFound)
		output.Encode("Object not found")
		return
	}
	output.Encode(object)
}

func (context *httpContext) getPollen(responseWriter http.ResponseWriter, request *http.Request) {
	output := json.NewEncoder(responseWriter)
	vars := mux.Vars(request)

	date, err := time.Parse(time.RFC3339, vars["date"])
	if err != nil {
		responseWriter.WriteHeader(400)
		output.Encode(err)
		return
	}

	pollenData, err := context.Repo.GetPollen(dataaccess.TimestampToDate(date))
	writeObject(responseWriter, output, pollenData, err)
}

func (context *httpContext) getPollenRange(responseWriter http.ResponseWriter, request *http.Request) {
	output := json.NewEncoder(responseWriter)

	from, err := time.Parse(time.RFC3339, request.FormValue("from"))
	if err != nil {
		output.Encode(err)
		return
	}

	to, err := time.Parse(time.RFC3339, request.FormValue("to"))
	if err != nil {
		output.Encode(err)
		return
	}

	pollenData, err := context.Repo.GetPollenFromRange(dataaccess.TimestampToDate(from), dataaccess.TimestampToDate(to))
	writeObject(responseWriter, output, pollenData, err)
}
