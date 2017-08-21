package dsunit

import (
	"github.com/viant/toolbox"
	"io/ioutil"
	"bytes"
)

type fileValueProvider struct{}

func (p *fileValueProvider) Get(context toolbox.Context, arguments ...interface{}) (interface{}, error) {
	filePath:= toolbox.AsString(arguments[0])

	fileContent , err := ioutil.ReadFile(filePath)
	if err != nil {
		panic(err)
	}
	content := bytes.TrimSpace(fileContent)
	result := string(content)
	return result,nil
}

func newFileValueProvider() toolbox.ValueProvider {
	var result toolbox.ValueProvider = &fileValueProvider{}
	return result
}
