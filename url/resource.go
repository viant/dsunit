package url

import (
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"strings"
)

//Resource represents a URL based resource, with enriched meta info
type Resource struct {
	URL         string     `description:"resource URL or relative or absolute path" required:"true"` //URL of resource
	Credentials string     `description:"credentials file"`                                          //name of credential file or credential key depending on implementation
	CustomKey   *AES256Key `description:" content encryption key"`
}

func (r *Resource) Init() (err error) {
	r.URL = url.Normalize(r.URL, file.Scheme)
	return err
}

//NewResource returns a new resource for provided URL, followed by optional credential, cache and cache expiryMs.
func NewResource(Params ...interface{}) *Resource {
	if len(Params) == 0 {
		return nil
	}
	var URL = toolbox.AsString(Params[0])
	URL = url.Normalize(URL, file.Scheme)

	var credential string
	if len(Params) > 1 {
		credential = toolbox.AsString(Params[1])
	}
	return &Resource{
		URL:         URL,
		Credentials: credential,
	}
}

//Decode decodes url's data into target, it support JSON and YAML exp.
func Decode(URL string, target interface{}) (err error) {

	ctx := context.Background()
	fs := afs.New()
	data, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return err
	}

	aMap := map[string]interface{}{}
	if strings.HasSuffix(URL, "yaml") {
		aMap = map[string]interface{}{}
		if err := yaml.Unmarshal(data, &aMap); err != nil {
			return err
		}
	} else {
		aMap = map[string]interface{}{}
		if err := json.Unmarshal(data, &aMap); err != nil {
			return err
		}
	}

	return toolbox.DefaultConverter.AssignConverted(target, aMap)
}
