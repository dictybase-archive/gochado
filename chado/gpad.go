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
	// Ontology namespace for linking qualifier, date, assigned_by
	// and with/from values of GPAD records.
	ontology string
}

// Create new instatnce of Sqlite structure
func NewChadoSqlite(dbh *sqlx.DB, parser *gochado.SqlParser, ont string) *Sqlite {
	return &Sqlite{parser, dbh, ont}
}

func (sqlite *Sqlite) AlterTables() {

}

func (sqlite *Sqlite) ResetTables() {

}

func (sqlite *Sqlite) BulkLoad() {
	parser := sqlite.sqlparser
	dbh := sqlite.dbh

	// -- Inserting new records
	// First get latest GAF records in another staging table
	dbh.MustExec(parser.GetSection("insert_latest_goa_from_staging"), sqlite.ontology, sqlite.ontology)
	// Now fill up the feature_cvterm
	dbh.MustExec(parser.GetSection("insert_feature_cvterm"))
	// Evidence code
	dbh.MustExec(parser.GetSection("insert_feature_cvtermprop_evcode"))
	// Extra references
	dbh.MustExec(parser.GetSection("insert_feature_cvterm_pub_reference"))
	sections := []string{
		"feature_cvtermprop_qualifier",
		"feature_cvtermprop_date",
		"feature_cvtermprop_assigned_by",
		"feature_cvtermprop_withfrom",
	}
	for _, s := range sections {
		s = "insert_" + s
		dbh.MustExec(parser.GetSection(s)+";", sqlite.ontology)
	}
}
