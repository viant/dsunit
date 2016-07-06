package dsunit

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

type serviceLocal struct {
	service       Service
	testManager   DatasetTestManager
	testDirectory string
}

//TestManager return a DatasetTestManager
func (s *serviceLocal) TestManager() DatasetTestManager {
	return s.testManager
}

func (s *serviceLocal) expandTestSchemaIfNeeded(candidate string) string {
	if strings.HasPrefix(candidate, TestSchema) {
		return s.testDirectory + candidate[len(TestSchema):]
	}
	return candidate
}

func (s *serviceLocal) expandTestSchemaURLIfNeeded(candidate string) string {
	if strings.HasPrefix(candidate, TestSchema) {
		return toolbox.FileSchema + s.testDirectory + candidate[len(TestSchema):]
	}
	return candidate
}

func (s *serviceLocal) registerDescriptors(dataStoreConfig *DatastoreConfig, manager dsc.Manager) string {
	result := ""
	if dataStoreConfig.Descriptors != nil {
		for i, tableDescriptor := range dataStoreConfig.Descriptors {
			dataStoreConfig.Descriptors[i].SchemaURL = s.expandTestSchemaURLIfNeeded(tableDescriptor.SchemaURL)
			manager.TableDescriptorRegistry().Register(&dataStoreConfig.Descriptors[i])
			result = result + "\t\tRegistered table: " + tableDescriptor.Table + "\n"
		}
	}
	return result
}

func (s *serviceLocal) registerMapping(dataStoreConfig *DatastoreConfig, manager dsc.Manager) string {
	result := ""
	if dataStoreConfig.DatasetMapping != nil {
		for name := range dataStoreConfig.DatasetMapping {
			datasetMapping := dataStoreConfig.DatasetMapping[name]
			s.testManager.RegisterDatasetMapping(name, &datasetMapping)
			result = result + "\t\tRegistered mapping: " + name + "\n"
			//register mapping table descriptor
			mappingTableDescriptor := manager.TableDescriptorRegistry().Get(datasetMapping.Table)
			mappingDescriptor := dsc.TableDescriptor{Table: name, PkColumns: mappingTableDescriptor.PkColumns, Autoincrement: mappingTableDescriptor.Autoincrement}
			manager.TableDescriptorRegistry().Register(&mappingDescriptor)
		}
	}
	return result
}

func (s *serviceLocal) loadConfigIfNeeded(datastoreConfig *DatastoreConfig) error {
	if datastoreConfig.ConfigURL != "" {
		datastoreConfig.ConfigURL = s.expandTestSchemaURLIfNeeded(datastoreConfig.ConfigURL)
		reader, _, err := toolbox.OpenReaderFromURL(datastoreConfig.ConfigURL)
		if err != nil {
			return fmt.Errorf("Failed to InitConfig - unable to open url %v due to %v", datastoreConfig.ConfigURL, err)

		}
		defer reader.Close()
		err = json.NewDecoder(reader).Decode(&datastoreConfig.Config)
		if err != nil {
			return fmt.Errorf("Failed to InitConfig - unable to decode url %v due to %v ", datastoreConfig.ConfigURL, err)
		}
	}
	datastoreConfig.Config.Init()
	return nil
}

func (s *serviceLocal) initDatastorFromConfig(datastoreConfig *DatastoreConfig) (string, error) {
	result := "Registered datastore: " + datastoreConfig.Datastore + "\n"
	err := s.loadConfigIfNeeded(datastoreConfig)
	if err != nil {
		return "", err
	}
	err = toolbox.ExpandParameters(s.testManager.MacroEvaluator(), datastoreConfig.Config.Parameters)
	for k, v := range datastoreConfig.Config.Parameters {
		datastoreConfig.Config.Parameters[k] = s.expandTestSchemaIfNeeded(v)
	}
	if err != nil {
		return "", fmt.Errorf("Failed to InitConfig - unable to expand config %v due to %v ", datastoreConfig.Config, err)
	}
	if datastoreConfig.Config.DriverName == "" {
		return "", fmt.Errorf("Invalid configuration missing driver %v %v", datastoreConfig.ConfigURL, datastoreConfig.Config)
	}

	factory, err := dsc.GetManagerFactory(datastoreConfig.Config.DriverName)
	if err != nil {
		return "", err
	}
	manager, err := factory.Create(&datastoreConfig.Config)
	if err != nil {
		return "", err
	}
	s.testManager.ManagerRegistry().Register(datastoreConfig.Datastore, manager)
	result = result + s.registerDescriptors(datastoreConfig, manager)
	result = result + s.registerMapping(datastoreConfig, manager)
	return result, nil
}

func (s *serviceLocal) Init(request *InitDatastoreRequest) *Response {
	message := ""
	for i := range request.DatastoreConfigs {
		initMessage, err := s.initDatastorFromConfig(&request.DatastoreConfigs[i])
		if err != nil {
			return newErrorResponse(err)
		}
		message += initMessage
	}

	for _, dataStoreConfig := range request.DatastoreConfigs {
		if dataStoreConfig.ClearDatastore {
			err := s.testManager.ClearDatastore(dataStoreConfig.AdminDbName, dataStoreConfig.Datastore)
			if err != nil {
				return newErrorResponse(dsUnitError{fmt.Sprintf("Failed to clear datastores %v, due to %v", dataStoreConfig.Datastore, err)})
			}
			message = message + fmt.Sprintf("Clear datastore  %v\n", dataStoreConfig.Datastore)
		}
	}
	if message == "" {
		return newErrorResponse(dsUnitError{fmt.Sprintf("Failed to init datastores, invalid request:%v", request)})
	}
	return newOkResponse(message)
}

func (s *serviceLocal) InitFromURL(url string) *Response {
	reader, _, err := toolbox.OpenReaderFromURL(s.expandTestSchemaURLIfNeeded(url))
	if err != nil {
		return newErrorResponse(err)
	}
	defer reader.Close()
	request := &InitDatastoreRequest{}
	err = json.NewDecoder(reader).Decode(&request)
	if err != nil {
		return newErrorResponse(dsUnitError{"Failed to init datastores, unable to decode payload from " + url + " due to:\n\t" + err.Error()})
	}
	return s.service.Init(request)
}

func (s *serviceLocal) ExecuteScripts(request *ExecuteScriptRequest) *Response {
	var message = ""
	if request.Scripts != nil {
		for _, script := range request.Scripts {
			var err error
			if len(script.SQLs) > 0 || len(script.Body) > 0 {
				_, err = s.testManager.Execute(&script)
			} else {
				_, err = s.testManager.ExecuteFromURL(script.Datastore, s.expandTestSchemaURLIfNeeded(script.URL))
			}
			if err != nil {
				return newErrorResponse(dsUnitError{"Failed to execut script on " + script.Datastore + " due to:\n\t" + err.Error()})
			}
			message = message + "Executed script " + script.URL + " on " + script.Datastore + "\n"
		}

	}
	if message == "" {
		return newErrorResponse(dsUnitError{fmt.Sprintf("Failed to execute scripts, invalid request:%v", request)})
	}
	return newOkResponse(message)
}

func (s *serviceLocal) ExecuteScriptsFromURL(url string) *Response {
	reader, _, err := toolbox.OpenReaderFromURL(s.expandTestSchemaURLIfNeeded(url))
	if err != nil {
		return newErrorResponse(err)
	}
	defer reader.Close()
	request := &ExecuteScriptRequest{}
	err = json.NewDecoder(reader).Decode(request)
	if err != nil {
		return newErrorResponse(dsUnitError{"Failed to execute scripts, unable to decode payload from " + url + " due to:\n\t" + err.Error()})
	}
	for i, script := range request.Scripts {
		if len(script.URL) > 0 && len(script.Body) == 0 {
			url := s.expandTestSchemaURLIfNeeded(script.URL)
			request.Scripts[i].URL = url

			if strings.HasPrefix(url, "file://") {
				file := url[len(toolbox.FileSchema):]
				bytes, err := ioutil.ReadFile(file)
				if err != nil {
					return newErrorResponse(dsUnitError{"Failed to execute script, unable to read file:" + file + " " + err.Error()})
				}

				request.Scripts[i].Body = string(bytes)
			}
		}
	}
	return s.service.ExecuteScripts(request)
}

func (s *serviceLocal) PrepareDatastore(request *PrepareDatastoreRequest) *Response {
	var totalInserted, totalUpdated, totalDeleted int
	var run = false
	message := ""

	for _, datasets := range request.Datasets {
		message += fmt.Sprintf("Prepared datastore %v with datasets:", datasets.Datastore)
		run = true
		inserted, updated, deleted, err := s.testManager.PrepareDatastore(&datasets)
		if err != nil {
			return newErrorResponse(dsUnitError{"Failed to prepare datastore due to:\n\t" + err.Error()})
		}
		totalInserted += inserted
		totalUpdated += updated
		totalDeleted += deleted
		for _, dataset := range datasets.Datasets {
			message += fmt.Sprintf("%v(%v), ", dataset.Table, len(dataset.Rows))
		}
		message += "\n\t"
	}
	if run {
		return newOkResponse(fmt.Sprintf("%vinserted: %v, updated: %v, deleted: %v\n", message, totalInserted, totalUpdated, totalDeleted))
	}
	return newErrorResponse(dsUnitError{fmt.Sprintf("Failed to prepare datastore, invalid request:%v", request)})
}

func (s *serviceLocal) PrepareDatastoreFromURL(url string) *Response {
	reader, _, err := toolbox.OpenReaderFromURL(s.expandTestSchemaIfNeeded(url))
	if err != nil {
		return newErrorResponse(err)
	}
	defer reader.Close()
	request := &PrepareDatastoreRequest{}
	err = json.NewDecoder(reader).Decode(&request)
	if err != nil {
		return newErrorResponse(dsUnitError{"Failed to prepare datastore, unable to decode payload from " + url + " due to:\n\t" + err.Error()})
	}
	return s.service.PrepareDatastore(request)
}

func (s *serviceLocal) PrepareDatastoreFor(datastore string, baseDir string, method string) *Response {
	datasets, err := s.buildDatasets(datastore, "prepare", baseDir, method)
	if err != nil {
		return newErrorResponse(err)
	}
	request := &PrepareDatastoreRequest{Datasets: []Datasets{*datasets}}
	return s.service.PrepareDatastore(request)
}

func (s *serviceLocal) ExpectDatasets(request *ExpectDatasetRequest) *Response {
	message := ""
	var hasViolations = false
	var run = false

	for _, datasets := range request.Datasets {
		message += fmt.Sprintf("\n\tVerified datastore %v with datasets:", datasets.Datastore)
		run = true
		violations, err := s.testManager.ExpectDatasets(request.CheckPolicy, &datasets)
		if err != nil {
			return newErrorResponse(dsUnitError{"Failed to verify expected datasets due to:\n\t" + err.Error()})
		}
		for _, dataset := range datasets.Datasets {
			message += fmt.Sprintf("%v(%v), ", dataset.Table, len(dataset.Rows))
		}
		message += "\n\t"
		if violations.HasViolations() {
			message = message + violations.String() + "\n"
			hasViolations = true
		}
	}
	if hasViolations {
		return newErrorResponse(dsUnitError{message})
	}

	if run {
		return newOkResponse(fmt.Sprintf("%vPassed", message))
	}
	return newErrorResponse(dsUnitError{fmt.Sprintf("Failed to verify expected datasets, invalid request:%v", request)})
}

func (s *serviceLocal) ExpectDatasetsFromURL(url string) *Response {
	reader, _, err := toolbox.OpenReaderFromURL(s.expandTestSchemaIfNeeded(url))
	if err != nil {
		return newErrorResponse(err)
	}
	defer reader.Close()
	request := &ExpectDatasetRequest{}
	err = json.NewDecoder(reader).Decode(&request)
	if err != nil {
		return newErrorResponse(dsUnitError{"Failed to prepare datastore, unable to decode payload from " + url + " due to:\n\t" + err.Error()})
	}
	return s.service.ExpectDatasets(request)
}

func (s *serviceLocal) ExpectDatasetsFor(datastore string, baseDir string, method string, checkPolicy int) *Response {
	datasets, err := s.buildDatasets(datastore, "expect", baseDir, method)
	if err != nil {
		return newErrorResponse(err)
	}
	request := &ExpectDatasetRequest{
		Datasets:    []Datasets{*datasets},
		CheckPolicy: checkPolicy,
	}
	return s.service.ExpectDatasets(request)
}

func (s *serviceLocal) GetTables(datastore string) []string {
	tables := s.testManager.RegisteredTables(datastore)
	for i := 0; i+1 < len(tables); i++ {
		for j := i + 1; j < len(tables); j++ {
			if len(tables[i]) < len(tables[j]) {
				temp := tables[i]
				tables[i] = tables[j]
				tables[j] = temp
			}
		}
	}
	return tables
}

func (s *serviceLocal) getTableForURL(datastore, url string) string {
	tables := s.GetTables(datastore)
	for _, table := range tables {
		if strings.Contains(url, "_"+table+".") {
			return table
		}
	}
	panic("Failed to match table in url")
}

func (s *serviceLocal) buildDatasets(datastore string, fragment string, baseDirectory string, method string) (*Datasets, error) {
	datasetFactory := s.testManager.DatasetFactory()
	tables := s.GetTables(datastore)
	if len(tables) == 0 {
		return nil, dsUnitError{"Unable to build dataset - no table register in dataset factory"}
	}
	baseDirectory = s.expandTestSchemaIfNeeded(baseDirectory)

	files, err := matchFiles(baseDirectory, method, fragment, tables)
	if err != nil {
		return nil, err
	}
	var datasets = make([]Dataset, 0)

	for _, file := range files {
		table := s.getTableForURL(datastore, file)
		datasetPoiner, err := datasetFactory.CreateFromURL(datastore, table, toolbox.FileSchema+file)
		if err != nil {
			return nil, err
		}
		dataset := *datasetPoiner
		datasets = append(datasets, dataset)
	}
	return &Datasets{
		Datastore: datastore,
		Datasets:  datasets,
	}, nil
}

//NewServiceLocal returns new local dsunit service, it takes test directory as argument.
func NewServiceLocal(testDirectory string) Service {
	datasetTestManager := NewDatasetTestManager()
	var localService = &serviceLocal{testManager: datasetTestManager, testDirectory: testDirectory}
	var result Service = localService
	localService.service = result
	return result
}
