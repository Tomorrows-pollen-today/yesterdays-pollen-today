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

// Scanner is an interface implemented by both sql.Row and sql.Rows.
type Scanner interface {
	Scan(dest ...interface{}) error
}

func rowToPollenSample(row Scanner) (*PollenSample, error) {
	pollenSample := &PollenSample{}
	err := row.Scan(&pollenSample.Date, &pollenSample.PollenCount, &pollenSample.PredictedPollenCount)
	return pollenSample, err
}

// InitDb Initializes database structure if it doesn't exist
func (repo *PollenRepository) InitDb() {
	_, err := repo.DB.Exec(`CREATE TABLE IF NOT EXISTS PollenArchive (
			Date TIMESTAMP PRIMARY KEY, PollenCount INT, PredictedPollenCount FLOAT)`)
	if err != nil {
		log.Println(fmt.Errorf("failed to create table: %v", err))
	}
	repo.PreparedStatements = make(map[string]*sql.Stmt)
	repo.prepareStatement("FetchPollen", "SELECT Date, PollenCount, PredictedPollenCount FROM PollenArchive WHERE Date = ?")
	repo.prepareStatement("FetchPollenRange", "SELECT Date, PollenCount, PredictedPollenCount FROM PollenArchive WHERE Date >= ? AND Date <= ?")
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
	existing, err := repo.GetPollen(pollen.Date)
	if err != nil {
		existing = &PollenSample{}
	}
	_, err = repo.DB.Exec("MERGE INTO PollenArchive (Date, PollenCount, PredictedPollenCount) VALUES (?, ?, ?)", pollen.Date, existing.PollenCount, pollen.PredictedPollenCount)
	if err != nil {
		log.Println(fmt.Errorf("failed insert data: %v", err))
	}
	return err
}

// UpsertPollenCount insert/updates the actual pollen count for a date
func (repo *PollenRepository) UpsertPollenCount(pollen *PollenSample) error {
	existing, err := repo.GetPollen(pollen.Date)
	if err != nil {
		existing = &PollenSample{}
	}
	_, err = repo.DB.Exec("MERGE INTO PollenArchive (Date, PollenCount, PredictedPollenCount) VALUES (?, ?, ?)", pollen.Date, pollen.PollenCount, existing.PredictedPollenCount)
	if err != nil {
		log.Println(fmt.Errorf("failed insert data: %v", err))
	}
	return err
}

// UpsertPollenSample insert/updates the actual pollen count and predicted pollen count for a date
func (repo *PollenRepository) UpsertPollenSample(pollen *PollenSample) error {
	_, err := repo.DB.Exec("MERGE INTO PollenArchive (Date, PollenCount, PredictedPollenCount) (SELECT ?, ?, ?)", pollen.Date, pollen.PollenCount, pollen.PredictedPollenCount)
	if err != nil {
		log.Println(fmt.Errorf("failed insert data: %v", err))
	}
	return err
}

// GetPollen fetch pollen data for a single date
func (repo *PollenRepository) GetPollen(date time.Time) (*PollenSample, error) {
	row := repo.PreparedStatements["FetchPollen"].QueryRow(date)
	return rowToPollenSample(row)
}

// GetPollenFromRange fetch pollen data for a range of dates
func (repo *PollenRepository) GetPollenFromRange(from time.Time, to time.Time) ([]*PollenSample, error) {
	var results []*PollenSample
	rows, err := repo.PreparedStatements["FetchPollenRange"].Query(from, to)
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
