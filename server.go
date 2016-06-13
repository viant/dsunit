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
package dsunit

import (
	"net/http"
	"fmt"
	"log"
	"github.com/viant/toolbox"
)

var version = "/v1/"
var initURI = version + "init"
var executeURI = version + "execute"
var prepareURI = version + "prepare"
var expectURI = version + "expect"


var errorHandler = func(router *toolbox.ServiceRouter, responseWriter http.ResponseWriter, httpRequest *http.Request, message string) {

	responseWriter.WriteHeader(http.StatusInternalServerError)
	err := router.WriteResponse(toolbox.NewJSONEncoderFactory(), &Response{Status:"error", Message:message}, httpRequest, responseWriter)
	if err != nil {
		log.Fatalf("Failed to write response :%v", err)
	}
}

//StartServer start dsunit server
func StartServer(port string) {
	var service  = NewServiceLocal("")
	serviceRouter := toolbox.NewServiceRouter(
		toolbox.ServiceRouting{
			HTTPMethod:"POST",
			URI:initURI,
			Handler:service.Init,
			Parameters:[]string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod:"POST",
			URI:executeURI,
			Handler:service.ExecuteScripts,
			Parameters:[]string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod:"POST",
			URI:prepareURI,
			Handler:service.PrepareDatastore,
			Parameters:[]string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod:"POST",
			URI:expectURI,
			Handler:service.ExpectDatasets,
			Parameters:[]string{"request"},
		},
	)


	http.HandleFunc("/", func(responseWriter http.ResponseWriter, httpRequest *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				errorHandler(serviceRouter, responseWriter, httpRequest,  fmt.Sprintf("%v", err))
			}
		}()
		err := serviceRouter.Route(responseWriter, httpRequest)
		if err != nil {
			errorHandler(serviceRouter, responseWriter, httpRequest,  fmt.Sprintf("%v", err))
		}
	})

	fmt.Printf("Started dsunit server on port %v\n", port)
	log.Fatal(http.ListenAndServe(":" + port, nil))
}

