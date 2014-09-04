package chado

import (
	"log"

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

type gpad struct {
	DbId       string `db:"dbid"`
	GoId       string
	Evcode     string
	AssignedBy string `db:"value"`
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
	p := sqlite.sqlparser
	dbh := sqlite.dbh

	// -- Inserting new records
	// First get latest GAF records in another staging table
	dbh.MustExec(p.GetSection("insert_latest_goa_from_staging"), sqlite.ontology)

	// Now check if its a fresh load or a merge load
	var count int
	err := dbh.QueryRowx(p.GetSection("count_all_gpads_from_chado"), sqlite.ontology).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	if count == 0 {
		// fresh load
		sqlite.runBulkInserts()
	} else {
		// merge load means two steps first insert the new record
		// then update the existing one
		gp := []gpad{}
		err := dbh.Select(&gp, p.GetSection("select_all_gpads_from_chado"), sqlite.ontology)
		if err != nil {
			log.Fatal(err)
		}
		for _, rec := range gp {
			mds := gochado.GetMD5Hash(rec.DbId + rec.GoId + rec.Evcode + rec.AssignedBy)
			var ct int
			err := dbh.QueryRowx(p.GetSection("count_temp_gpad_new_by_checksum"), mds).Scan(&ct)
			if err != nil {
				log.Fatal(err)
			}
			if ct > 0 {
				// record exist, mark for update step
				dbh.MustExec(p.GetSection("update_temp_gpad_new_by_checksum"), mds)
			}
		}
		sqlite.runBulkInserts()
	}
}

func (sqlite *Sqlite) runBulkInserts() {
	p := sqlite.sqlparser
	dbh := sqlite.dbh

	// Now fill up the feature_cvterm
	dbh.MustExec(p.GetSection("insert_feature_cvterm"))
	// Evidence code
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_evcode"))
	// Extra references
	dbh.MustExec(p.GetSection("insert_feature_cvterm_pub_reference"))
	sections := []string{
		"feature_cvtermprop_qualifier",
		"feature_cvtermprop_date",
		"feature_cvtermprop_assigned_by",
		"feature_cvtermprop_withfrom",
	}
	for _, s := range sections {
		s = "insert_" + s
		dbh.MustExec(p.GetSection(s)+";", sqlite.ontology)
	}
}
