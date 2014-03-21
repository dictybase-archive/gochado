package chado

import (
    "github.com/dictybase/gochado"
    "github.com/jmoiron/sqlx"
)

// Sqlite backend for loading GPAD data from staging to chado tables
type Sqlite struct {
    // ini parser for file with SQL statements
    sqlparser *gochado.SqlParser
    // instance of database handle
    dbh *sqlx.DB
}

// Create new instatnce of Sqlite structure
func NewChadoSqlite(dbh *sqlx.DB, parser *gochado.SqlParser) *Sqlite {
    return &Sqlite{parser, dbh}
}

func (sqlite *Sqlite) AlterTables() {

}

func (sqlite *Sqlite) ResetTables() {

}

func (sqlite *Sqlite) BulkLoad() {
    parser := sqlite.sqlparser
    dbh := sqlite.dbh
    // First get latest GAF records in another staging table
    dbh.Execf(parser.GetSection("insert_latest_goa_from_staging") + ";")
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
