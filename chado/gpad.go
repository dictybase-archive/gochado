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
	// Ontology namespace for anonymous cvterms
	anonCv string
	// Db namespace for anonymous cvterms
	anonDb string
}

type gpad struct {
	DbId       string `db:"dbid"`
	GoId       string
	Evcode     string
	AssignedBy string `db:"value"`
}

type anon struct {
	Name         string
	Digest       string
	Id           string
	Db           string
	Relationship string
}

// Create new instatnce of Sqlite structure
func NewChadoSqlite(dbh *sqlx.DB, parser *gochado.SqlParser, ont string, acv string, adb string) *Sqlite {
	return &Sqlite{parser, dbh, ont, acv, adb}
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
	// The records with their date field updated will be transfered
	dbh.MustExec(p.GetSection("insert_latest_goa_from_staging"), sqlite.ontology)

	// Now check if its a fresh load or a merge load
	var count int
	// Count all gpads including those linked with anon cvterms
	err := dbh.QueryRowx(p.GetSection("count_all_gpads_from_chado"), sqlite.anonCv, sqlite.ontology).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	if count == 0 {
		// fresh load
		sqlite.RunBulkInserts()
	} else {
		// tag the records in staging that could be updated
		// these will also includes records with annotation extensions
		sqlite.MarkUpdatable()
		// merge load means two steps first insert the new record
		// then update the existing one
		sqlite.RunBulkInserts()
		sqlite.RunBulkUpdates()
	}
}

func (sqlite *Sqlite) MarkUpdatable() {
	p := sqlite.sqlparser
	dbh := sqlite.dbh
	gp := []gpad{}
	err := dbh.Select(&gp, p.GetSection("select_all_gpads_from_chado"), sqlite.ontology, sqlite.anonCv, sqlite.ontology)
	if err != nil {
		log.Fatal(err)
	}
	for _, rec := range gp {
		mds := gochado.GetMD5Hash(rec.DbId + rec.GoId + rec.Evcode + rec.AssignedBy)
		var ct int
		// check if the record is new or is an update
		err := dbh.QueryRowx(p.GetSection("count_temp_gpad_new_by_checksum"), mds).Scan(&ct)
		if err != nil {
			log.Fatal(err)
		}
		if ct > 0 {
			// record exist, mark for update step
			dbh.MustExec(p.GetSection("update_temp_gpad_new_by_checksum"), mds)
		}
	}
}

func (sqlite *Sqlite) RunBulkUpdates() {
	p := sqlite.sqlparser
	dbh := sqlite.dbh
	// Refresh the values of all updated gpad entries
	// Extra references
	dbh.MustExec(p.GetSection("insert_feature_cvterm_pub_reference"), 1)
	sections := []string{
		"insert_feature_cvtermprop_qualifier",
		"insert_feature_cvtermprop_withfrom",
	}
	for _, s := range sections {
		dbh.MustExec(p.GetSection(s), sqlite.ontology, 1)
	}

	// Now update the date fields
	type date struct {
		Id   int    `db:"feature_cvterm_id"`
		Date string `db:"date_curated"`
	}
	gpd := []date{}
	err := dbh.Select(&gpd, p.GetSection("select_updated_gpad_date"))
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range gpd {
		dbh.MustExec(p.GetSection("update_feature_cvtermprop_date"), d.Date, sqlite.ontology, d.Id)
	}
}

func (sqlite *Sqlite) RunBulkInserts() {
	sqlite.createAnonCvterms()
	sqlite.insertExtraIdentifiers()
	sqlite.insertNonAnonGpad()
	sqlite.insertAnonFeatCvt()
	sqlite.insertAnonImplExpl()
}

// insert database/sequence identifers that comes in the identifier
// part of annotation extensions
func (sqlite *Sqlite) insertExtraIdentifiers() {
	p := sqlite.sqlparser
	dbh := sqlite.dbh
	dbh.MustExec(p.GetSection("insert_anon_cvterm_db_identifier"))
	dbh.MustExec(p.GetSection("insert_anon_cvterm_dbxref_identifier"))
}

func (sqlite *Sqlite) insertAnonCvprop() {
	sqlite.dbh.MustExec(sqlite.sqlparser.GetSection("insert_anon_cvtermprop_extension"), sqlite.anonCv)
}

//insert all gpad entries expect the ones with extensions
func (sqlite *Sqlite) insertNonAnonGpad() {
	p := sqlite.sqlparser
	dbh := sqlite.dbh
	// Now fill up the feature_cvterm
	dbh.MustExec(p.GetSection("insert_feature_cvterm"))
	// Evidence code
	dbh.MustExec(p.GetSection("insert_feature_cvtermprop_evcode"), 0)
	// Extra references
	dbh.MustExec(p.GetSection("insert_feature_cvterm_pub_reference"), 0)
	sections := []string{
		"insert_feature_cvtermprop_qualifier",
		"insert_feature_cvtermprop_date",
		"insert_feature_cvtermprop_assigned_by",
		"insert_feature_cvtermprop_withfrom",
	}
	for _, s := range sections {
		dbh.MustExec(p.GetSection(s), sqlite.ontology, 0)
	}
}

// insert implicit and explicit columns
func (sqlite *Sqlite) insertAnonImplExpl() {
	p := sqlite.sqlparser
	dbh := sqlite.dbh
	tbl := [][]string{
		[]string{"insert_anon_feature_cvtermprop_evcode", sqlite.anonCv},
		[]string{"insert_anon_feature_cvtermprop_qualifier", sqlite.ontology, sqlite.anonCv},
		[]string{"insert_anon_feature_cvtermprop_date", sqlite.ontology, sqlite.anonCv},
		[]string{"insert_anon_feature_cvtermprop_withfrom", sqlite.ontology, sqlite.anonCv},
		[]string{"insert_anon_feature_cvtermprop_assigned_by", sqlite.ontology, sqlite.anonCv},
		[]string{"insert_anon_feature_cvterm_pub_reference", sqlite.anonCv},
	}

	for _, entry := range tbl {
		if len(entry) == 2 {
			dbh.MustExec(p.GetSection(entry[0]), entry[1], 0)
		} else {
			dbh.MustExec(p.GetSection(entry[0]), entry[1], entry[2], 0)
		}
	}
}

// insert new feature_cvterm with anon terms
func (sqlite *Sqlite) insertAnonFeatCvt() {
	sqlite.dbh.Exec(sqlite.sqlparser.GetSection("insert_anon_feature_cvterm"), sqlite.anonCv)
}

//insert anon cvterms relationships
func (sqlite *Sqlite) insertAnonRelationships() {
	p := sqlite.sqlparser
	dbh := sqlite.dbh
	dbh.MustExec(p.GetSection("insert_anon_cvterm_rel_original"), "ro", sqlite.anonCv)
	dbh.Exec(p.GetSection("insert_anon_cvterm_rel_extension"), sqlite.anonCv)
}

func (sqlite *Sqlite) createAnonCvterms() {
	p := sqlite.sqlparser
	dbh := sqlite.dbh
	an := []anon{}
	err := dbh.Select(&an, p.GetSection("select_anon_cvterm"))
	if err != nil {
		log.Fatal(err)
	}
	for _, a := range an {
		q := p.GetSection("update_temp_with_anon_cvterm")
		dbh.MustExec(q, a.Name, a.Digest, a.Id, a.Db, a.Relationship)
	}
	dbh.MustExec(p.GetSection("insert_anon_cvterm_in_dbxref"), sqlite.anonDb)
	dbh.MustExec(p.GetSection("insert_anon_cvterm"), sqlite.anonCv, sqlite.anonDb)
}
