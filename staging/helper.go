package staging

import (
	"errors"
	"log"
	"regexp"
	"strings"

	"github.com/dictybase/gochado"
	"github.com/jmoiron/sqlx"
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

type GpadHelper struct {
	*gochado.ChadoHelper
	// ini parser instance
	*gochado.SqlParser
	// slice holds list of sections in the ini file
	sections []string
	// slice holds list of tables
	tables []string
	// map of buckets for holding rows of data
	buckets map[string]*gochado.DataBucket
	// map of rank values to identify record with different evidence codes
	ranks map[string]int
}

func (gh *GpadHelper) AddExtensionDataRow(d string, digest interface{}) {
	// Verify if extension column is annotated
	if _, ok := gh.buckets["gpad_extension"]; !ok {
		log.Fatal("key *gpad_extension* is not found in bucket")
	}

	if strings.Contains(d, "|") {
		// Handle multiple values
		aextn := strings.Split(d, "|")
		for i, value := range aextn {
			if m := er.FindStringSubmatch(value); m != nil {
				gext := make(map[string]interface{})
				dbxref := strings.Split(m[2], ":")
				gext["db"] = dbxref[0]
				gext["id"] = dbxref[1]
				gext["relationship"] = m[1]
				gext["digest"] = digest
				gext["rank"] = i + 1
				gh.buckets["gpad_extension"].Push(gext)
			}
		}
	} else {
		if m := er.FindStringSubmatch(d); m != nil {
			gext := make(map[string]interface{})
			dbxref := strings.Split(m[2], ":")
			gext["db"] = dbxref[0]
			gext["id"] = dbxref[1]
			gext["relationship"] = m[1]
			gext["digest"] = digest
			gh.buckets["gpad_extension"].Push(gext)
		}
	}
}

func (gh *GpadHelper) AddExtraReferenceRow(pr []*PubRecord, digest interface{}) {
	if len(pr) > 1 {
		if _, ok := gh.buckets["gpad_reference"]; !ok {
			log.Fatal("key *gpad_reference* is not found in bucket")
		}
		for _, r := range pr[1:] {
			gref := make(map[string]interface{})
			gref["digest"] = digest
			gref["publication_id"] = r.id
			gref["pubplace"] = r.pubplace
			gh.buckets["gpad_reference"].Push(gref)
		}
	}
}

func (gh *GpadHelper) AddQualifierFromRow(d string, digest interface{}) {
	if _, ok := gh.buckets["gpad_qualifier"]; !ok {
		log.Fatal("key *gpad_qualifier* is not found in buckets")
	}

	qualifier := make([]string, 0)
	if strings.Contains(d, "|") {
		qualifier = append(qualifier, strings.Split(d, "|")...)
	} else {
		qualifier = append(qualifier, d)
	}
	for i, value := range qualifier {
		gq := make(map[string]interface{})
		gq["digest"] = digest
		gq["qualifier"] = value
		gq["rank"] = i
		gh.buckets["gpad_qualifier"].Push(gq)
	}
}

func (gh *GpadHelper) AddWithfromRow(d string, digest interface{}) {
	if len(d) > 0 {
		if _, ok := gh.buckets["gpad_withfrom"]; !ok {
			log.Fatal("key *gpad_withfrom* is not found in bucket")
		}
		wfrom := make([]string, 0)
		if strings.Contains(d, "|") {
			wfrom = append(wfrom, strings.Split(d, "|")...)
		} else {
			wfrom = append(wfrom, d)
		}
		for i, value := range wfrom {
			gwfrom := make(map[string]interface{})
			gwfrom["digest"] = digest
			gwfrom["withfrom"] = value
			gwfrom["rank"] = i
			gh.buckets["gpad_withfrom"].Push(gwfrom)
		}
	}
}

func GetStagingLoader(dbh *sqlx.DB, parser *gochado.SqlParser, loader string) (gochado.StagingLoader, error) {
	//list of ini sections
	sec := make([]string, 0)
	tbl := make([]string, 0)
	//slice of data buckets keyed by staging table names.
	//each element of bucket slice is map type that represents a row of data.
	//keys of the map represents column names.
	buc := make(map[string]*gochado.DataBucket)
	for _, section := range parser.Sections() {
		if strings.HasPrefix(section, "create_table_temp_") {
			n := strings.Replace(section, "create_table_temp_", "", 1)
			buc[n] = gochado.NewDataBucket()
			tbl = append(tbl, strings.Replace(section, "create_table_", "", 1))
			sec = append(sec, section)
		}
	}
	switch loader {
	case "sqlite":
		return nil, errors.New("loader sqlite is not implemented")
	case "postgres":
		return &Postgres{
			&GpadHelper{
				gochado.NewChadoHelper(dbh),
				parser,
				sec,
				tbl,
				buc,
				make(map[string]int),
			},
		}, nil
	default:
		return nil, errors.New("loader does not exist")
	}
}
