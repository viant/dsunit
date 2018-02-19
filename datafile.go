package dsunit

import (
	"path"
	"strings"
)



//DatafileInfo represent data file
type DatafileInfo struct {
	Filename string
	Name string
	Ext string
	Prefix string
	Postfix string
}


//NewDatafileInfo returns new datafile info if supplied filedinfo matches prefix, postfix
func NewDatafileInfo(filename, prefix, postfix string) *DatafileInfo {
	var result = &DatafileInfo{
		Filename: filename,
		Prefix:prefix,
		Postfix:postfix,
	}
	name := filename
	if prefix != "" {
		if ! strings.HasPrefix(name, prefix) {
			return nil
		}
		name = string(name[len(prefix):])
	}
	ext := path.Ext(name)
	if len(ext) > 0 {
		name = string(name[:len(name)-len(ext)])
		result.Ext = string(ext[1:])
	}
	if postfix != "" {
		if ! strings.HasSuffix(name, postfix) {
			return nil
		}
		name = string(name[:len(name)-len(postfix)])
	}
	result.Name = name
	return result
}



