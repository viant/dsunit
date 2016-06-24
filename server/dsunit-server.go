/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

// Package server - Remote testing dsunit server
package server

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
