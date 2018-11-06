package dsunit

import (
	"fmt"
	"github.com/viant/toolbox"
	"log"
	"net/http"
)

var version = "/v2/"
var initURI = version + "init"
var registerURI = version + "register"
var recreateURI = version + "recreate"
var mappingURI = version + "mapping"
var scriptURI = version + "script"
var sqlURI = version + "sql"
var prepareURI = version + "prepare"
var expectURI = version + "expect"
var queryURI = version + "query"
var freezeURI = version + "freeze"
var dumpURI = version + "dump"
var sequenceURI = version + "sequence"

var errorHandler = func(router *toolbox.ServiceRouter, responseWriter http.ResponseWriter, httpRequest *http.Request, message string) {
	err := router.WriteResponse(toolbox.NewJSONEncoderFactory(), &BaseResponse{Status: "error", Message: message}, httpRequest, responseWriter)
	if err != nil {
		log.Fatalf("failed to write response :%v", err)
	}
}

//StartServer start dsunit server
func StartServer(port string) {
	var service = New()
	serviceRouter := toolbox.NewServiceRouter(
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        registerURI,
			Handler:    service.Register,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        recreateURI,
			Handler:    service.Recreate,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        mappingURI,
			Handler:    service.AddTableMapping,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        initURI,
			Handler:    service.Init,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        scriptURI,
			Handler:    service.RunScript,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        sqlURI,
			Handler:    service.RunSQL,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        prepareURI,
			Handler:    service.Prepare,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        expectURI,
			Handler:    service.Expect,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        queryURI,
			Handler:    service.Query,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        sequenceURI,
			Handler:    service.Sequence,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        freezeURI,
			Handler:    service.Freeze,
			Parameters: []string{"request"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        dumpURI,
			Handler:    service.Dump,
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
