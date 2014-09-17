package staging

import (
	"bytes"
	"fmt"
	"log"
	"regexp"
	"strconv"
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

// Sqlite backend for loading GPAD in staging tables
type Sqlite struct {
	*gochado.ChadoHelper
	// ini parser instance
	sqlparser *gochado.SqlParser
	// slice holds list of sections in the ini file
	sections []string
	// slice holds list of tables
	tables []string
	// map of buckets for holding rows of data
	buckets map[string]*gochado.DataBucket
	// map of rank values to identify record with different evidence codes
	ranks map[string]int
}

func NewStagingSqlite(dbh *sqlx.DB, parser *gochado.SqlParser) *Sqlite {
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
	return &Sqlite{gochado.NewChadoHelper(dbh), parser, sec, tbl, buc, make(map[string]int)}
}

func (sqlite *Sqlite) AddDataRow(row string) {
	//ignore blank lines
	if br.MatchString(row) {
		return
	}
	// ignore comment line
	if strings.HasPrefix(row, "!") {
		return
	}
	d := strings.Split(row, "\t")
	refs := make([]string, 0)
	if strings.Contains(d[4], "|") {
		refs = append(refs, strings.Split(d[4], "|")...)
	} else {
		refs = append(refs, d[4])
	}
	goid := strings.Split(d[3], ":")[1]
	evcode := strings.Split(d[5], ":")[1]
	// slice of PubRecord struct type
	pr := NormaLizePubRecord(refs)

	gpad := make(map[string]interface{})
	// d[1] Unique identifier such as gene product identifier
	// d[9] Assigned by, database which made the annotation
	// evcode Evidence code
	gpad["digest"] = gochado.GetMD5Hash(d[1] + goid + evcode + d[9])
	gpad["id"] = d[1]
	gpad["goid"] = goid
	gpad["publication_id"] = pr[0].id
	gpad["pubplace"] = pr[0].pubplace
	gpad["evidence_code"] = evcode
	gpad["date_curated"] = d[8]
	gpad["assigned_by"] = d[9]
	// A requirement for chado table feature_cvterm where feature_id,
	// cvterm_id and pub_id and rank have to be unique.
	rdigest := gochado.GetMD5Hash(d[1] + goid + pr[0].id + pr[0].pubplace)
	if r, ok := sqlite.ranks[rdigest]; ok {
		sqlite.ranks[rdigest] = r + 1
		gpad["rank"] = r + 1
	} else {
		sqlite.ranks[rdigest] = 0
		gpad["rank"] = 0
	}
	if _, ok := sqlite.buckets["gpad"]; !ok {
		log.Fatal("key *gpad* is not found in bucket")
	}
	sqlite.buckets["gpad"].Push(gpad)

	sqlite.addQualifierFromRow(d[2], gpad["digest"])
	sqlite.addExtraReferenceRow(pr, gpad["digest"])
	if len(d[6]) > 0 {
		sqlite.addWithfromRow(d[6], gpad["digest"])
	}
	if len(d[10]) > 0 {
		sqlite.addExtensionDataRow(d[10], gpad["digest"])
	}
}

func (sqlite *Sqlite) addQualifierFromRow(d string, digest interface{}) {
	if _, ok := sqlite.buckets["gpad_qualifier"]; !ok {
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
		sqlite.buckets["gpad_qualifier"].Push(gq)
	}
}

func (sqlite *Sqlite) addWithfromRow(d string, digest interface{}) {
	if len(d) > 0 {
		if _, ok := sqlite.buckets["gpad_withfrom"]; !ok {
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
			sqlite.buckets["gpad_withfrom"].Push(gwfrom)
		}
	}
}

func (sqlite *Sqlite) addExtraReferenceRow(pr []*PubRecord, digest interface{}) {
	if len(pr) > 1 {
		if _, ok := sqlite.buckets["gpad_reference"]; !ok {
			log.Fatal("key *gpad_reference* is not found in bucket")
		}
		for _, r := range pr[1:] {
			gref := make(map[string]interface{})
			gref["digest"] = digest
			gref["publication_id"] = r.id
			gref["pubplace"] = r.pubplace
			sqlite.buckets["gpad_reference"].Push(gref)
		}
	}
}

func (sqlite *Sqlite) addExtensionDataRow(d string, digest interface{}) {
	// Verify if extension column is annotated
	if _, ok := sqlite.buckets["gpad_extension"]; !ok {
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
				sqlite.buckets["gpad_extension"].Push(gext)
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
			sqlite.buckets["gpad_extension"].Push(gext)
		}
	}
}

func (sqlite *Sqlite) CreateTables() {
	dbh := sqlite.ChadoHelper.ChadoHandler
	var csec []string
	for _, section := range sqlite.sections {
		csec = append(csec, sqlite.sqlparser.GetSection(section)+";")
	}
	dbh.MustExec(strings.Join(csec, "\n"))
}

func (sqlite *Sqlite) DropTables() {
}

func (sqlite *Sqlite) AlterTables() {
}

func (sqlite *Sqlite) BulkLoad() {
	//Here is how it works...
	//Get name of each staging table
	for name := range sqlite.buckets {
		b := sqlite.buckets[name]
		if b.Count() == 0 { // no data
			continue
		}
		//Get the first element from bucket and then extract columns names
		columns := make([]string, 0)
		for col := range b.GetByPosition(0) {
			columns = append(columns, col)
		}
		tbl := "temp_" + name
		pstmt := fmt.Sprintf("INSERT INTO %s(%s)", tbl, strings.Join(columns, ","))
		var str bytes.Buffer
		for _, element := range b.Elements() {
			fstmt := fmt.Sprintf("%s VALUES(%s);\n", pstmt, strings.Join(ElementToValueString(element, columns), ","))
			str.WriteString(fstmt)
		}
		sqlite.ChadoHelper.ChadoHandler.MustExec(str.String())
	}
}

func ElementToValueString(element map[string]interface{}, columns []string) []string {
	values := make([]string, 0)
	for _, name := range columns {
		if v, ok := element[name]; ok {
			switch d := v.(type) {
			case int:
				values = append(values, strconv.Itoa(d))
			case string:
				values = append(values, "'"+d+"'")
			}
		} else {
			values = append(values, "")
		}
	}
	return values
}
