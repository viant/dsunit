package main

import (
	"flag"

	"github.com/viant/dsunit"
	//Place all your datastore driver here
	_ "github.com/go-sql-driver/mysql"
)

const (
	defaultPort = "8071"
	usage       = "dsunit-server port"
)

func main() {
	var port string
	flag.StringVar(&port, "port", defaultPort, usage)
	dsunit.StartServer(port)
}
