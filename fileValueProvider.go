package dsunit

import (
	"github.com/viant/toolbox"
	"io/ioutil"
)

const filePath = "/data/logs/stream/tableIdCounter.data"

type fileValueProvider struct{}

func (p *fileValueProvider) Get(context toolbox.Context, arguments ...interface{}) (interface{}, error) {

	value , err := ioutil.ReadFile(filePath)
	if err != nil {
		panic(err)
	}
	result := string(value)
	return result,nil
}

func newFileValueProvider() toolbox.ValueProvider {
	var result toolbox.ValueProvider = &fileValueProvider{}
	return result
}
