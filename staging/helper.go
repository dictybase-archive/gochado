package staging

import (
	"regexp"
	"strings"
)

var br = regexp.MustCompile(`^\s+$`)
var er = regexp.MustCompile(`(\w+)\((\S+)\)`)

// Publication record with id and namespace
type PubRecord struct {
	id       string
	pubplace string
}

func NormaLizePubRecord(pubs []string) []*PubRecord {
	pr := make([]*PubRecord, 0)
	for _, r := range pubs {
		out := strings.Split(r, ":")
		if out[0] == "PMID" {
			pr = append(pr, &PubRecord{out[1], "PubMed"})
			continue
		}
		pr = append(pr, &PubRecord{out[1], out[0]})
	}
	return pr
}
