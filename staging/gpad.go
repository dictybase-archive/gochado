package staging

import (
    "bytes"
    "fmt"
    "github.com/dictybase/gochado"
    "github.com/jmoiron/sqlx"
    "log"
    "strings"
)

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
}

func NewSqlite(dbh *sqlx.DB, parser *gochado.SqlParser) *Sqlite {
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
    return &Sqlite{gochado.NewChadoHelper(dbh), parser, sec, tbl, buc}
}

func (sqlite *Sqlite) AddDataRow(row string) {
    if strings.HasPrefix(row, "!") {
        return
    }
    d := strings.Split(row, "\t")
    var pref string
    refs := make([]string, 0)
    if strings.Contains(d[4], "|") {
        refs = append(refs, strings.Split(d[4], "|")...)
    } else {
        refs = append(refs, d[4])
    }

    gpad := make(map[string]string)
    gpad["digest"] = gochado.GetMD5Hash(d[1] + d[2] + d[3] + pref + d[5] + d[8] + d[9])
    gpad["id"] = d[1]
    gpad["qualifier"] = d[2]
    gpad["goid"] = d[3]
    gpad["publication_id"] = refs[0]
    gpad["evidence_code"] = d[5]
    gpad["assigned_by"] = d[8]
    gpad["date_curated"] = d[9]
    if _, ok := sqlite.buckets["gpad"]; !ok {
        log.Fatal("key *gpad* is not found in bucket")
    }
    sqlite.buckets["gpad"].Push(gpad)

    if len(refs) > 1 {
        if _, ok := sqlite.buckets["gpad_reference"]; !ok {
            log.Fatal("key *gpad_reference* is not found in bucket")
        }
        for _, value := range refs {
            gref := make(map[string]string)
            gref["digest"] = gpad["digest"]
            gref["publication_id"] = value
            sqlite.buckets["gpad_reference"].Push(gref)
        }
    }

    if len(d[6]) > 0 {
        if _, ok := sqlite.buckets["gpad_withfrom"]; !ok {
            log.Fatal("key *gpad_withfrom* is not found in bucket")
        }
        wfrom := make([]string, 0)
        if strings.Contains(d[6], "|") {
            wfrom = append(wfrom, strings.Split(d[6], "|")...)
        } else {
            wfrom = append(wfrom, d[6])
        }
        for _, value := range wfrom {
            gwfrom := make(map[string]string)
            gwfrom["digest"] = gpad["digest"]
            gwfrom["withfrom"] = value
            sqlite.buckets["gpad_withfrom"].Push(gwfrom)
        }
    }
}

func (sqlite *Sqlite) CreateTables() {
    dbh := sqlite.ChadoHelper.ChadoHandler
    var csec []string
    for _, section := range sqlite.sections {
        csec = append(csec, sqlite.sqlparser.GetSection(section)+";")
    }
    dbh.Execf(strings.Join(csec, "\n"))
}

func (sqlite *Sqlite) DropTables() {
}

func (sqlite *Sqlite) AlterTables() {
}

func (sqlite *Sqlite) BulkLoad() {
    //Here is how it works...
    //Get name of each staging table
    for name := range sqlite.buckets {
        b, ok := sqlite.buckets[name]
        if !ok {
            log.Fatalf("Unable to retrieve bucket named %s", name)
        }
        //Get the first element from bucket and then extract columns names
        columns := make([]string, 0)
        for col := range b.GetByPosition(0) {
            columns = append(columns, col)
        }
        pstmt := fmt.Sprintf("INSERT INTO %s(%s)", name, strings.Join(columns, ","))
        var str bytes.Buffer
        for _, element := range b.Elements() {
            fstmt := fmt.Sprintf("%s VALUES(%s);\n", pstmt, strings.Join(ElementToValueString(element, columns), ","))
            str.WriteString(fstmt)
        }
        sqlite.ChadoHelper.ChadoHandler.Execf(str.String())
    }
}

func ElementToValueString(element map[string]string, columns []string) []string {
    values := make([]string, 0)
    for _, name := range columns {
        if v, ok := element[name]; ok {
            values = append(values, v)
        } else {
            values = append(values, "")
        }
    }
    return values
}
