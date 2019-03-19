package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// PollenTypeDto has a pollen id and name
type PollenTypeDto struct {
	PollenType dataaccess.PollenType `json:"pollenid"`
	PollenName string                `json:"name"`
}

type httpContext struct {
	Repo *dataaccess.PollenRepository
}

func main() {
	// Change directory to the same as executable
	executable, _ := os.Executable()
	exPath := filepath.Dir(executable)
	os.Chdir(exPath)

	logfile, err := os.OpenFile("pollen-api.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer logfile.Close()

	log.SetOutput(logfile)
	log.Printf("Starting api at %v", time.Now())

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

	apiRouter.HandleFunc("/pollentype", context.getPollenTypes)

	apiRouter.HandleFunc("/location/{location}", context.getLocation)

	apiRouter.HandleFunc("/location", context.searchLocation).
		Queries(
			"country", "{country}",
			"city", "{city}")

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

	http.ListenAndServe(":8001", trailingSlashMiddleware(router))
}

func trailingSlashMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = strings.TrimSuffix(r.URL.Path, "/")
		next.ServeHTTP(w, r)
	})
}

// Write an object to the output stream as a JSON blob. Handles the most common error codes as well.
func writeObject(responseWriter http.ResponseWriter, output *json.Encoder, object interface{}, err error) {
	if err != nil {
		responseWriter.WriteHeader(http.StatusInternalServerError)
		log.Println(err)
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

// Get a list of the current pollen types handled by the API.
func (context *httpContext) getPollenTypes(responseWriter http.ResponseWriter, request *http.Request) {
	output := json.NewEncoder(responseWriter)

	types, err := context.Repo.GetPollenTypes()
	if err != nil {
		responseWriter.WriteHeader(http.StatusInternalServerError)
		output.Encode(err)
		return
	}

	result := make([]PollenTypeDto, len(types))

	for i, pollenType := range types {
		result[i] = PollenTypeDto{
			PollenType: pollenType,
			PollenName: pollenType.String(),
		}
	}

	output.Encode(result)
}

// Get a location by an id.
func (context *httpContext) getLocation(responseWriter http.ResponseWriter, request *http.Request) {
	output := json.NewEncoder(responseWriter)
	vars := mux.Vars(request)

	locationID, err := strconv.Atoi(vars["location"])
	if err != nil {
		responseWriter.WriteHeader(http.StatusBadRequest)
		output.Encode(err)
		return
	}

	location, err := context.Repo.GetLocation(locationID)

	writeObject(responseWriter, output, location, err)
}

// Find a location by either city of country. Useful to get the location id for use with getPollen.
func (context *httpContext) searchLocation(responseWriter http.ResponseWriter, request *http.Request) {
	output := json.NewEncoder(responseWriter)

	country := request.FormValue("country")
	city := request.FormValue("city")
	if country == "" && city == "" {
		responseWriter.WriteHeader(http.StatusBadRequest)
		return
	}

	location, err := context.Repo.SearchLocation(country, city)

	writeObject(responseWriter, output, location, err)
}

// Get the pollen count as well as the predicted pollen count for a given date, pollen type and location.
// If the date is the string "tomorrow", the correct date for tomorrow will be used.
// Pollen type and location can be omitted for now, and will simply use grass pollen for copenhagen
// for backwards compatibility. It is deprecated and will be removed once tomorrowspollen.today is
// updated to use the new parameters.
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
