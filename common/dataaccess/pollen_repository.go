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

// PollenDate holds a pollencount for a given date
type PollenDate struct {
	PollenCount          *int32
	PredictedPollenCount *float32
	Date                 time.Time
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

}

// TimestampToDate converts a timestamp to a date used in the repository
func TimestampToDate(timestamp time.Time) time.Time {
	return time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, time.UTC)
}