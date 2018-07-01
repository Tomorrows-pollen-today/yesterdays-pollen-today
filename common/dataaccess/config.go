package dataaccess

import (
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/amsokol/ignite-go-client/binary/v1"
)

// DbConnectionConfig holds data for connecting to Ignite
type DbConnectionConfig struct {
	SQLConnectionString string
	ConnInfo            ignite.ConnInfo
	CacheName           string
}

func getConfig() *DbConnectionConfig {
	var config *DbConnectionConfig
	if _, err := toml.DecodeFile("db.toml", &config); err != nil {
		fmt.Println(err)
		panic(err)
	}
	return config
}
