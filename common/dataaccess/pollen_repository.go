package dataaccess

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/amsokol/ignite-go-client/binary/v1"
	// Here to import the sql driver
	_ "github.com/amsokol/ignite-go-client/sql"
)

// GetConnection initializes a new db connection
func GetConnection() (*PollenRepository, error) {
	config := getConfig()
	// connect
	db, err := sql.Open("ignite", config.SQLConnectionString)
	if err != nil {
		log.Fatalf("failed connect to db: %v", err)
		return nil, err
	}

	repo := &PollenRepository{
		DB: db,
	}

	client, err := ignite.Connect(config.ConnInfo)
	if err != nil {
		log.Fatalf("failed connect to server: %v", err)
		return repo, err
	}
	defer client.Close()
	err = client.CacheGetOrCreateWithName(config.CacheName)
	if err != nil {
		log.Fatalf("failed to get or create cache: %v", err)
		return repo, err
	}
	return repo, nil
}

// PollenRepository a repository for pollen
type PollenRepository struct {
	DB                 *sql.DB
	PreparedStatements map[string]*sql.Stmt
}

// PollenSample holds a pollencount for a given date
type PollenSample struct {
	PollenCount          int32
	PollenType           PollenType
	PredictedPollenCount float32
	Date                 time.Time
	Location             Location
}

// PollenType denotes a type of pollen
type PollenType int

const (
	// PollenTypeGrass grass
	PollenTypeGrass PollenType = 0
	//PollenTypeBirch birch
	PollenTypeBirch PollenType = 1
)

func (pollenType PollenType) String() string {
	switch pollenType {
	case PollenTypeGrass:
		return "Grass"
	case PollenTypeBirch:
		return "Birch"
	default:
		return ""
	}
}

// Location is a location where pollen is measured and predicted
type Location struct {
	Location int
	City     string
	Country  string
}

// Scanner is an interface implemented by both sql.Row and sql.Rows.
type Scanner interface {
	Scan(dest ...interface{}) error
}

func rowToPollenSample(row Scanner) (*PollenSample, error) {
	pollenSample := &PollenSample{}
	err := row.Scan(&pollenSample.Date,
		&pollenSample.PollenType,
		&pollenSample.Location.Location,
		&pollenSample.Location.Country,
		&pollenSample.Location.City,
		&pollenSample.PollenCount,
		&pollenSample.PredictedPollenCount)
	return pollenSample, err
}

// InitDb Initializes database structure if it doesn't exist
func (repo *PollenRepository) InitDb() {
	var err error
	_, err = repo.DB.Exec(`
		CREATE TABLE IF NOT EXISTS PollenArchive (
			Date TIMESTAMP,
			PollenType INT,
			Location INT,
			PollenCount INT, 
			PredictedPollenCount FLOAT,
			PRIMARY KEY (Date, PollenType, Location)
		)`)
	if err != nil {
		panic(fmt.Errorf("Failed to create PollenArchive: %v", err))
	}
	_, err = repo.DB.Exec(`
		CREATE TABLE IF NOT EXISTS Locations (
			Location INT PRIMARY KEY,
			Country VARCHAR,
			City VARCHAR
		)`)
	if err != nil {
		panic(fmt.Errorf("Failed to create Locations: %v", err))
	}
	_, err = repo.DB.Exec(`
		CREATE INDEX IF NOT EXISTS LocationLookup ON Locations (
			Country ASC,
			City ASC
		)`)
	if err != nil {
		panic(fmt.Errorf("Failed to create index on Locations: %v", err))
	}

	repo.PreparedStatements = make(map[string]*sql.Stmt)
	repo.prepareStatement("FetchPollen", `
		SELECT 
			Date,
			PollenType,
			Locations.Location,
			Locations.Country,
			Locations.City,
			PollenCount, 
			PredictedPollenCount 
		FROM PollenArchive 
		JOIN Locations on PollenArchive.Location = Locations.Location
		WHERE 
			Date = ? AND  
			PollenType = ? AND 
			PollenArchive.Location = ?`)
	repo.prepareStatement("FetchPollenRange", `
		SELECT 
			Date,
			PollenType,
			Locations.Location,
			Locations.Country,
			Locations.City,
			PollenCount, 
			PredictedPollenCount 
		FROM PollenArchive 
		JOIN Locations on PollenArchive.Location = Locations.Location
		WHERE 
			Date >= ? AND 
			Date <= ? AND 
			PollenType = ? AND 
			PollenArchive.Location = ?`)
}

func (repo *PollenRepository) prepareStatement(key string, statement string) {
	query, err := repo.DB.Prepare(statement)
	if err != nil {
		log.Println(fmt.Errorf("failed prepare query: %v", err))
	} else {
		repo.PreparedStatements[key] = query
	}
}

// Close closes connections to the database
func (repo *PollenRepository) Close() {
	repo.DB.Close()
}

// UpsertPredictedPollenCount insert/updates the actual pollen count for a date
func (repo *PollenRepository) UpsertPredictedPollenCount(pollen *PollenSample) error {
	existing, err := repo.GetPollen(pollen.Date, pollen.PollenType, pollen.Location.Location)
	if err != nil {
		existing = &PollenSample{}
	}
	_, err = repo.DB.Exec(`
		MERGE INTO PollenArchive (Date, PollenType, Location, PollenCount, PredictedPollenCount) 
		VALUES (?, ?, ?, ?, ?)`,
		pollen.Date, pollen.PollenType, pollen.Location.Location, existing.PollenCount, pollen.PredictedPollenCount)
	if err != nil {
		log.Println(fmt.Errorf("failed insert data: %v", err))
	}
	return err
}

// UpsertPollenCount insert/updates the actual pollen count for a date
func (repo *PollenRepository) UpsertPollenCount(pollen *PollenSample) error {
	existing, err := repo.GetPollen(pollen.Date, pollen.PollenType, pollen.Location.Location)
	if err != nil {
		existing = &PollenSample{}
	}
	_, err = repo.DB.Exec(`
		MERGE INTO PollenArchive (Date, PollenType, Location, PollenCount, PredictedPollenCount) 
		VALUES (?, ?, ?, ?, ?)`,
		pollen.Date, pollen.PollenType, pollen.Location.Location, pollen.PollenCount, existing.PredictedPollenCount)
	if err != nil {
		log.Println(fmt.Errorf("failed insert data: %v", err))
	}
	return err
}

// UpsertPollenSample insert/updates the actual pollen count and predicted pollen count for a date
func (repo *PollenRepository) UpsertPollenSample(pollen *PollenSample) error {
	_, err := repo.DB.Exec(`
		MERGE INTO PollenArchive (Date, PollenType, Location, PollenCount, PredictedPollenCount) 
		(SELECT ?, ?, ?, ?, ?)`,
		pollen.Date, pollen.PollenType, pollen.Location.Location, pollen.PollenCount, pollen.PredictedPollenCount)
	if err != nil {
		log.Println(fmt.Errorf("failed insert data: %v", err))
	}
	return err
}

// GetPollen fetch pollen data for a single date
func (repo *PollenRepository) GetPollen(date time.Time, pollenType PollenType, location int) (*PollenSample, error) {
	row := repo.PreparedStatements["FetchPollen"].QueryRow(date, pollenType, location)
	return rowToPollenSample(row)
}

// GetPollenFromRange fetch pollen data for a range of dates
func (repo *PollenRepository) GetPollenFromRange(from time.Time, to time.Time, pollenType PollenType, location int) ([]*PollenSample, error) {
	var results []*PollenSample
	rows, err := repo.PreparedStatements["FetchPollenRange"].Query(from, to, pollenType, location)
	defer rows.Close()
	if err != nil {
		log.Println(fmt.Errorf("failed to get data: %v", err))
	}
	for rows.Next() {
		pollenSample, err := rowToPollenSample(rows)
		if err != nil {
			log.Println(fmt.Errorf("failed to get data: %v", err))
		}
		results = append(results, pollenSample)
	}
	err = rows.Err()
	if err != nil {
		log.Println(fmt.Errorf("failed to get data: %v", err))
	}
	return results, err
}

// TimestampToDate converts a timestamp to a date used in the repository
func TimestampToDate(timestamp time.Time) time.Time {
	return time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, time.UTC)
}
