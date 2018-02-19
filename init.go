package dsunit

import (
	"github.com/viant/assertly"
)

func init() {

	assertly.ValueProviderRegistry.Register("sql", newQueryValueProvider())
	assertly.ValueProviderRegistry.Register("seq", newSequenceValueProvider(":seq"))
	assertly.ValueProviderRegistry.Register("pos", newSequenceValueProvider(":pos"))
	//	registry.Register("fromQuery", newBgQueryProvider())

}
