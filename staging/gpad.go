package staging

import (
    "github.com/dictybase/gochado"
    "github.com/jmoiron/sqlx"
    "strings"
)

// Sqlite backend for loading GPAD in staging tables
type Sqlite struct {
    *gochado.ChadoHelper
    sqlparser *gochado.SqlParser
    sections  []string
    buckets   map[string]*gochado.DataBucket
}

func NewSqlite(dbh *sqlx.DB, parser *gochado.SqlParser) *Sqlite {
    sec := make([]string, 0)
    buc := make(map[string]*gochado.DataBucket)
    for i, section := range parser.Sections() {
        if strings.HasPrefix(section, "create_table_temp_") {
            n := strings.Replace(section, "create_table_temp_", "", 1)
            buc[n] = gochado.NewDataBucket()
            sec[i] = n
        }
    }
    return &Sqlite{gochado.NewChadoHelper(dbh), parser, sec, buc}
}

func (sqlite *Sqlite) AddDataRow(row string) {
    d := strings.Split(row, "\t")
    var pref string
    var refs []string
    if strings.Contains(d[4], "|") {
        refs = strings.Split(d[4], "|")
    } else {
        refs[0] = d[4]
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
    sqlite.buckets["gpad"].Push(gpad)

    if len(refs) > 1 {
        for _, value := range refs {
            gref := make(map[string]string)
            gref["digest"] = gpad["digest"]
            gref["publication_id"] = value
            sqlite.buckets["gpad_reference"].Push(gref)
        }
    }

    if len(d[6]) > 0 {
        var wfrom []string
        if strings.Contains(d[6], "|") {
            wfrom = strings.Split(d[6], "|")
        } else {
            wfrom[0] = d[6]
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
}
