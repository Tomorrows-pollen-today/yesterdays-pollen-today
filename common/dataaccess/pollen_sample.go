package dataaccess

import (
	"database/sql"
	"time"
)

// PollenSample holds a pollencount for a given date
type PollenSample struct {
	PollenType           PollenType
	PollenCount          int
	PredictedPollenCount float32
	Date                 time.Time
	Location             Location
}

// pollenSampleSQL is used to get data from SQL. It is then converted to a PollenSample
type pollenSampleSQL struct {
	PollenType           PollenType
	PollenCount          sql.NullInt64
	PredictedPollenCount sql.NullFloat64
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
