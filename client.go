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

// Package dsunit -
package dsunit

import (
	"github.com/viant/toolbox"
)

type serviceClient struct {
	serviceLocal
	serverURL string
}

func getResponseOrErrorResponse(response *Response, err error) *Response {
	if err != nil {
		return newErrorResponse(err)
	}
	return response
}

func (c *serviceClient) Init(request *InitDatastoreRequest) *Response {
	for i := range request.DatastoreConfigs {
		_, err := c.initDatastorFromConfig(&request.DatastoreConfigs[i])
		if err != nil {
			return newErrorResponse(err)
		}
	}
	response := &Response{}
	err := toolbox.RouteToService("post", c.serverURL+initURI, request, response)
	return getResponseOrErrorResponse(response, err)
}

func (c *serviceClient) ExecuteScripts(request *ExecuteScriptRequest) *Response {
	response := &Response{}
	err := toolbox.RouteToService("post", c.serverURL+executeURI, request, response)
	return getResponseOrErrorResponse(response, err)
}

func (c *serviceClient) PrepareDatastore(request *PrepareDatastoreRequest) *Response {
	response := &Response{}
	err := toolbox.RouteToService("post", c.serverURL+prepareURI, request, response)
	return getResponseOrErrorResponse(response, err)
}

func (c *serviceClient) ExpectDatasets(request *ExpectDatasetRequest) *Response {
	response := &Response{}
	err := toolbox.RouteToService("post", c.serverURL+expectURI, request, response)
	return getResponseOrErrorResponse(response, err)
}

//NewServiceClient returns a new dsunit service client
func NewServiceClient(testDirectory, serverURL string) Service {
	datasetTestManager := NewDatasetTestManager()
	var localService = serviceLocal{testManager: datasetTestManager, testDirectory: testDirectory}
	var dsUnitClient = &serviceClient{serviceLocal: localService, serverURL: serverURL}
	var result Service = dsUnitClient
	localService.service = result
	dsUnitClient.service = result
	return result
}
