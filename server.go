package dsunit

import (
	"fmt"
	"log"
	"net/http"
	"github.com/viant/toolbox"
)

var version = "/v1/"
var initURI = version + "init"
var executeURI = version + "execute"
var prepareURI = version + "prepare"
var expectURI = version + "expect"

var errorHandler = func(router *toolbox.ServiceRouter, responseWriter http.ResponseWriter, httpRequest *http.Request, message string) {

	responseWriter.WriteHeader(http.StatusInternalServerError)
	responseWriter.Header().Set("Error", message)
	err := router.WriteResponse(toolbox.NewJSONEncoderFactory(), &Response{Status: "error", Message: message}, httpRequest, responseWriter)
	if err != nil {
		log.Fatalf("Failed to write response :%v", err)
	}
}

//StartServer start dsunit server
func StartServer(port string) {
	var service = NewServiceLocal("")
	serviceRouter := toolbox.NewServiceRouter(
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        initURI,
			Handler:    service.Init,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        executeURI,
			Handler:    service.ExecuteScripts,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        prepareURI,
			Handler:    service.PrepareDatastore,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        expectURI,
			Handler:    service.ExpectDatasets,
			Parameters: []string{"request"},
		},
	)

	http.HandleFunc("/", func(responseWriter http.ResponseWriter, httpRequest *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				errorHandler(serviceRouter, responseWriter, httpRequest, fmt.Sprintf("%v", err))
			}
		}()
		err := serviceRouter.Route(responseWriter, httpRequest)
		if err != nil {
			errorHandler(serviceRouter, responseWriter, httpRequest, fmt.Sprintf("%v", err))
		}
	})

	fmt.Printf("Started dsunit server on port %v\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
