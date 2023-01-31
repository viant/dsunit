package dsunit

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/scy/kms"
	"math/rand"
	"strings"
	"time"
)

type ObfuscationMethod string

const (
	ObfuscationMethodShuffle    = "shuffle"
	ObfuscationMethodDictionary = "dictionary"
	ObfuscationMethodCipher     = "cipher"
)

type Obfuscation struct {
	Columns       []string
	Method        ObfuscationMethod
	DictionaryURL string
	Dictionary    []string
	Key           *kms.Key
}

func (o *Obfuscation) Init(ctx context.Context) {
	if o.Method == ObfuscationMethodCipher && o.Key == nil || o.Key.Scheme == "" {
		if o.Key == nil {
			o.Key = &kms.Key{}
		}
		if o.Key.Scheme == "" {
			o.Key.Scheme = "blowfish"
		}
		if o.Key.Auth == "" {
			o.Key.Auth = "default"
		}
	}

	if o.DictionaryURL == "" || len(o.Dictionary) > 0 {
		return
	}
	fs := afs.New()
	data, _ := fs.DownloadWithURL(ctx, o.DictionaryURL)
	lines := bytes.Split(data, []byte("\n"))
	for _, line := range lines {
		o.Columns = append(o.Columns, strings.TrimSpace(string(line)))
	}
}

func (o *Obfuscation) Obfuscate(ctx context.Context, value string) (string, error) {
	switch o.Method {
	case "", ObfuscationMethodShuffle:
		return o.shuffle(value), nil
	case ObfuscationMethodDictionary:
		if len(o.Dictionary) == 0 {
			return value, fmt.Errorf("dictionary was empty: %v", o.DictionaryURL)
		}
		return o.randDictionaryValue(), nil
	case ObfuscationMethodCipher:
		cipher, err := kms.Lookup(o.Key.Scheme)
		if err != nil {
			return "", err
		}
		enc, err := cipher.Encrypt(ctx, o.Key, []byte(value))
		if err != nil {
			return "", err
		}
		return base64.StdEncoding.EncodeToString(enc), nil
	default:
		return "", fmt.Errorf("unsupported obfuscation method:%v", o.Method)
	}
}

func (o *Obfuscation) randDictionaryValue() string {
	rand.Seed(time.Now().UnixNano())
	index := int(rand.Int31()) % len(o.Dictionary)
	return o.Dictionary[index]
}

func (o *Obfuscation) shuffle(value string) string {
	rand.Seed(time.Now().UnixNano())
	data := []byte(value)
	rand.Shuffle(len(data), func(i, j int) { data[i], data[j] = data[j], data[i] })
	return string(data)
}
