package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/ovh/tsl/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
