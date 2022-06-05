package url_test

import (
	"github.com/stretchr/testify/assert"
	afsfile "github.com/viant/afs/file"
	"github.com/viant/afs/url"
	dsurl "github.com/viant/dsunit/url"
	"github.com/viant/toolbox"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestNewResource(t *testing.T) {

	{
		var resource = dsurl.NewResource("https://raw.githubusercontent.com/viant/toolbox/master/LICENSE.txt", "credential_test")
		assert.Equal(t, "https://raw.githubusercontent.com/viant/toolbox/master/LICENSE.txt", resource.URL)
		assert.Equal(t, "credential_test", resource.Credentials)
	}
}

func TestResource_YamlDecode(t *testing.T) {
	tempDir := os.TempDir()
	var filename1 = path.Join(tempDir, "resource1.yaml")
	var filename2 = path.Join(tempDir, "resource2.yaml")

	assert.Nil(t, toolbox.RemoveFileIfExist(filename1, filename2))
	defer assert.Nil(t, toolbox.RemoveFileIfExist(filename1, filename2))

	var aMap = map[string]interface{}{
		"a": 1,
		"b": "123",
		"c": []int{1, 3, 6},
	}

	file, err := os.OpenFile(filename1, os.O_CREATE|os.O_RDWR, 0644)
	if assert.Nil(t, err) {
		err = toolbox.NewYamlEncoderFactory().Create(file).Encode(aMap)
		assert.Nil(t, err)
	}

	{
		var resourceData = make(map[string]interface{})
		location := url.Normalize(filename1, afsfile.Scheme)
		err = dsurl.Decode(location, &resourceData)
		assert.Nil(t, err)
		assert.EqualValues(t, resourceData["a"], 1)
		assert.EqualValues(t, resourceData["b"], "123")
	}

	// Warning! A YAML file cannot contain tabs as indentation.
	YAML := `init:
  defaultUser: &defaultUser
    name: bob
    age: 18
pipeline:
  test:
    init:
      users:
        <<: *defaultUser
        age: 24
    action: print
    message: I got $users`

	err = ioutil.WriteFile(filename2, []byte(YAML), 0644)
	assert.Nil(t, err)

	{

		var resourceData = make(map[string]interface{})
		location := url.Normalize(filename2, afsfile.Scheme)
		err = dsurl.Decode(location, &resourceData)
		assert.Nil(t, err)

		if normalized, err := toolbox.NormalizeKVPairs(resourceData); err == nil {
			resourceData = toolbox.AsMap(normalized)
		} else {
			assert.Nil(t, err)
		}

		//fmt.Println("resourceData: ", resourceData)
		assert.EqualValues(t, resourceData["init"].(map[string]interface{})["defaultUser"].(map[string]interface{})["age"], 18)
		assert.EqualValues(t, resourceData["init"].(map[string]interface{})["defaultUser"].(map[string]interface{})["name"], "bob")

		assert.EqualValues(t, resourceData["pipeline"].(map[string]interface{})["test"].(map[string]interface{})["init"].(map[string]interface{})["users"].(map[string]interface{})["age"], 24)
		assert.EqualValues(t, resourceData["pipeline"].(map[string]interface{})["test"].(map[string]interface{})["init"].(map[string]interface{})["users"].(map[string]interface{})["name"], "bob")

		assert.EqualValues(t, resourceData["pipeline"].(map[string]interface{})["test"].(map[string]interface{})["action"], "print")
		assert.EqualValues(t, resourceData["pipeline"].(map[string]interface{})["test"].(map[string]interface{})["message"], "I got $users")

		//assert.EqualValues(t, resourceData["b"], "123")
	}

}

func TestResource_JsonDecode(t *testing.T) {
	tempDir := os.TempDir()
	var filename = path.Join(tempDir, "resource.json")

	assert.Nil(t, toolbox.RemoveFileIfExist(filename))
	defer assert.Nil(t, toolbox.RemoveFileIfExist(filename))

	var aMap = map[string]interface{}{
		"a": 1,
		"b": "123",
	}
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if assert.Nil(t, err) {
		err = toolbox.NewJSONEncoderFactory().Create(file).Encode(aMap)
		assert.Nil(t, err)
	}

	var resourceData = make(map[string]interface{})
	location := url.Normalize(filename, afsfile.Scheme)
	err = dsurl.Decode(location, &resourceData)
	assert.Nil(t, err)
	assert.EqualValues(t, resourceData["a"], 1)
	assert.EqualValues(t, resourceData["b"], "123")

}
