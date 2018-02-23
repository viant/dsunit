package dsunit

const (
	//FullTableDatasetCheckPolicy policy will drive comparison of all actual datastore data
	FullTableDatasetCheckPolicy = 0
	//SnapshotDatasetCheckPolicy policy will drive comparison of subset of  actual datastore data that is is listed in expected dataset
	SnapshotDatasetCheckPolicy = 1
)
