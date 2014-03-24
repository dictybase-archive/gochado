package chado

import (
    "github.com/dictybase/gochado"
    "github.com/jmoiron/sqlx"
    "log"
)

// Sqlite backend for loading GPAD data from staging to chado tables
type Sqlite struct {
    // ini parser for file with SQL statements
    sqlparser *gochado.SqlParser
    // instance of database handle
    dbh *sqlx.DB
    // instance of Organism, should have genus and species defined
    *gochado.Organism
}

// Create new instatnce of Sqlite structure
func NewChadoSqlite(dbh *sqlx.DB, parser *gochado.SqlParser, org *gochado.Organism) *Sqlite {
    return &Sqlite{parser, dbh, org}
}

func (sqlite *Sqlite) AlterTables() {

}

func (sqlite *Sqlite) ResetTables() {

}

func (sqlite *Sqlite) BulkLoad() {
    parser := sqlite.sqlparser
    dbh := sqlite.dbh

    //Check for presence of and goa record
    type entries struct{ Counter int }
    e := entries{}
    err := dbh.Get(&e, parser.GetSection("select_latest_goa_count_chado"), sqlite.Organism.Genus, sqlite.Organism.Species)
    if err != nil {
        log.Fatalf("should have run the query error: %s", err)
    }
    grecord := 0
    // if there is any then get the date field of the latest one
    if e.Counter > 0 {
        type lt struct{ Latest int }
        l := lt{}
        err := dbh.Get(&l, parser.GetSection("select_latest_goa_bydate_chado"), sqlite.Organism.Genus, sqlite.Organism.Species)
        if err != nil {
            log.Fatalf("should have run the query error: %s", err)
        }
        grecord = l.Latest
    }
    // First get latest GAF records in another staging table
    dbh.Execf(parser.GetSection("insert_latest_goa_from_staging"), grecord)
    // Now fill up the feature_cvterm
    dbh.Execf(parser.GetSection("insert_feature_cvterm") + ";")
    sections := []string{
        "featureprop_evcode",
        "featureprop_qualifier",
        "featureprop_date",
        "featurepub_reference",
        "featureprop_assigned_by",
        "featureprop_withfrom",
    }
    for _, s := range sections {
        dbh.Execf(parser.GetSection(s) + ";")
    }
}
