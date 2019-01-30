package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
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
	// For temporary backwards compatibility. Is deprecated.
	apiRouter.HandleFunc("/pollen/{date}", context.getPollen)

	apiRouter.HandleFunc("/pollen/{date}", context.getPollen).
		Queries(
			"pollentype", "{pollentype}",
			"location", "{location}")

	apiRouter.HandleFunc("/pollen", context.getPollenRange).
		Queries(
			"from", "{from}",
			"to", "{to}",
			"pollentype", "{pollentype}",
			"location", "{location}")

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

	var date time.Time
	if vars["date"] == "tomorrow" {
		date = time.Now().AddDate(0, 0, 1)
	} else {
		var err error
		date, err = time.Parse(time.RFC3339, vars["date"])
		if err != nil {
			responseWriter.WriteHeader(http.StatusBadRequest)
			output.Encode(err)
			return
		}
	}

	// Parse pollen type
	pollenType, err := strconv.Atoi(request.FormValue("pollentype"))
	if err != nil {
		// TODO: return error when obsolete is removed entirely
		responseWriter.Header().Set("X-Obsolete-pollentype", "Calling this endpoint without declaring pollentype in query is obsolete")
		pollenType = int(dataaccess.PollenTypeGrass)
	}

	// Parse location
	location, err := strconv.Atoi(request.FormValue("location"))
	if err != nil {
		// TODO: return error when obsolete is removed entirely
		responseWriter.Header().Set("X-Obsolete-location", "Calling this endpoint without declaring location in query is obsolete")
		location = 0
	}

	pollenData, err := context.Repo.GetPollen(
		dataaccess.TimestampToDate(date),
		dataaccess.PollenType(pollenType),
		location)
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

	// Parse pollen type
	pollenType, err := strconv.Atoi(request.FormValue("pollentype"))
	if err != nil {
		responseWriter.WriteHeader(http.StatusBadRequest)
		output.Encode(err)
		return
	}

	// Parse location
	location, err := strconv.Atoi(request.FormValue("location"))
	if err != nil {
		responseWriter.WriteHeader(http.StatusBadRequest)
		output.Encode(err)
		return
	}

	pollenData, err := context.Repo.GetPollenFromRange(
		dataaccess.TimestampToDate(from),
		dataaccess.TimestampToDate(to),
		dataaccess.PollenType(pollenType),
		location)
	writeObject(responseWriter, output, pollenData, err)
}
