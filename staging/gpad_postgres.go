package staging

import (
	"bytes"
	"fmt"
	"log"
	"strings"

	"github.com/dictybase/gochado"
)

// postgres backend for loading GPAD in staging tables
type Postgres struct {
	*GpadHelper
}

func (pg *Postgres) AddDataRow(row string) {
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
	if r, ok := pg.ranks[rdigest]; ok {
		pg.ranks[rdigest] = r + 1
		gpad["rank"] = r + 1
	} else {
		pg.ranks[rdigest] = 0
		gpad["rank"] = 0
	}
	if _, ok := pg.buckets["gpad"]; !ok {
		log.Fatal("key *gpad* is not found in bucket")
	}
	pg.buckets["gpad"].Push(gpad)

	pg.AddQualifierFromRow(d[2], gpad["digest"])
	pg.AddExtraReferenceRow(pr, gpad["digest"])
	if len(d[6]) > 0 {
		pg.AddWithfromRow(d[6], gpad["digest"])
	}
	if len(d[10]) > 0 {
		pg.AddExtensionDataRow(d[10], gpad["digest"])
	}
}

func (pg *Postgres) CreateTables() {
	dbh := pg.ChadoHandler
	var csec []string
	for _, section := range pg.sections {
		csec = append(csec, pg.GetSection(section)+";")
	}
	dbh.MustExec(strings.Join(csec, "\n"))
}

func (pg *Postgres) DropTables() {
}

func (pg *Postgres) AlterTables() {
}

func (pg *Postgres) ResetTables() {
	for _, name := range pg.tables {
		pg.ChadoHelper.ChadoHandler.MustExec("DELETE FROM " + name)
	}
}

func (pg *Postgres) PreLoad() {
}

func (pg *Postgres) PostLoad() {
}

func (pg *Postgres) BulkLoad() {
	pg.PreLoad()
	//Here is how it works...
	//Get name of each staging table
	for name := range pg.buckets {
		b := pg.buckets[name]
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
		pg.ChadoHelper.ChadoHandler.MustExec(str.String())
	}
	pg.PostLoad()
}
