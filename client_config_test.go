package dsunit

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/dsc"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRemoteLegacyConnectionConfigSurvivesJSON(t *testing.T) {
	var seen []map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]interface{}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		seen = append(seen, request)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Status":"ok"}`))
	}))
	defer server.Close()
	original := &RegisterRequest{Datastore: "fixture", Config: &dsc.Config{DriverName: "sqlite3", Descriptor: "[url]", Parameters: map[string]interface{}{"url": "fixture.db"}}}
	client := NewServiceClient(server.URL)
	require.Equal(t, "ok", client.Register(original).Status)
	require.Equal(t, "ok", client.Init(&InitRequest{Datastore: "fixture", RegisterRequest: original, Admin: original}).Status)
	for _, request := range seen {
		config := request["Config"].(map[string]interface{})
		require.Equal(t, "sqlite3", config["Driver"])
		require.Equal(t, "[url]", config["DSN"])
	}
	require.Equal(t, "sqlite3", seen[1]["Admin"].(map[string]interface{})["Config"].(map[string]interface{})["Driver"])
	require.Empty(t, original.Config.Driver)
	require.Empty(t, original.Config.DSN)
}
