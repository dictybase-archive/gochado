package chado

import (
	"bytes"
	"encoding/gob"
	"log"

	"github.com/GeertJohan/go.rice"
	"github.com/dictybase/gochado"
	"github.com/dictybase/gochado/staging"
	"github.com/dictybase/testchado"
	. "github.com/dictybase/testchado/matchers"
)

type chadoTest struct {
	rice   *rice.Box
	chado  *testchado.Sqlite
	parser *gochado.SqlParser
}

// Load fixtures for testing GPAD loading. It loads the following fixtures
//  Gene ids
//	Reference/Publications
//	Ontology terms/ids
//	Various ontology terms under gene_ontology_association namespace
//  Cv and db namespace for anonymous cvterms
func LoadGpadChadoFixtureSqlite(chado testchado.DBManager, b *rice.Box) {
	//get the gob file
	r, err := b.Open("fixture.gob")
	defer r.Close()
	if err != nil {
		log.Fatal("Could not get gob file fixture.gob")

	}
	// Now decode and get the data
	dec := gob.NewDecoder(r)
	var genes []string
	err = dec.Decode(&genes)
	if err != nil {
		log.Fatal(err)
	}
	f := gochado.NewGpadFixtureLoader(chado)
	// Gene ids
	_ = f.LoadGenes(genes)

	var gorefs []string
	err = dec.Decode(&gorefs)
	if err != nil {
		log.Fatal(err)
	}
	// Pub ids
	_ = f.LoadPubIds(gorefs)

	var goids map[string][]string
	err = dec.Decode(&goids)
	if err != nil {
		log.Fatal(err)
	}
	// Load GO ids
	_ = f.LoadGoIds(goids)
	_ = f.LoadMiscCvterms("gene_ontology_association")

	// Cv and db namespace for anonymous cvterms
	f.LoadAnonNamespaces()
	// Extension cvterms
	var cvtslice []map[string]string
	err = dec.Decode(&cvtslice)
	if err != nil {
		log.Fatal(err)
	}
	_ = f.LoadCvterms(cvtslice)

	// Dbxrefs
	var xrefs []string
	err = dec.Decode(&xrefs)
	if err != nil {
		log.Fatalf("decoding err in xref %s", err)
	}
	_ = f.LoadDbxrefs(xrefs)

	// Relationship cvterms
	var rslice []map[string]string
	err = dec.Decode(&rslice)
	if err != nil {
		log.Fatalf("unable to decode %s\n", err)
	}
	_ = f.LoadCvterms(rslice)
}

// Loads GPAD test file to staging tables
func LoadGpadStagingSqlite(chado testchado.DBManager, b *rice.Box, sql string) {
	// test struct creation and table handling
	staging := staging.NewStagingSqlite(chado.DBHandle(), gochado.NewSqlParserFromString(sql))
	staging.CreateTables()

	// test data buffering
	gpstr, err := b.String("test.gpad")
	if err != nil {
		log.Fatal(err)
	}
	buff := bytes.NewBufferString(gpstr)
	for {
		line, err := buff.ReadString('\n')
		if err != nil {
			break
		}
		staging.AddDataRow(line)
	}
	//bulkload testing
	staging.BulkLoad()
}

// Loads updated GPAD test file
func LoadUpdatedGpadStagingSqlite(chado testchado.DBManager, b *rice.Box, sql string) {
	// test struct creation and table handling
	staging := staging.NewStagingSqlite(chado.DBHandle(), gochado.NewSqlParserFromString(sql))
	staging.ResetTables()

	// test data buffering
	gpstr, err := b.String("test_updated.gpad")
	if err != nil {
		log.Fatal(err)
	}
	buff := bytes.NewBufferString(gpstr)
	for {
		line, err := buff.ReadString('\n')
		if err != nil {
			break
		}
		staging.AddDataRow(line)
	}
	//bulkload testing
	staging.BulkLoad()
}

func setUpSqliteTest() *chadoTest {
	chado := testchado.NewSQLiteManager()
	RegisterDBHandler(chado)
	chado.DeploySchema()
	chado.LoadPresetFixture("eco")
	b := rice.MustFindBox("../data")
	str, err := b.String("sqlite_gpad.ini")
	if err != nil {
		log.Fatalf("could not open file sqlite_gpad.ini from rice box error:%s", err)
	}
	// Loads test gpad file in staging
	LoadGpadStagingSqlite(chado, b, str)
	// Loads fixtures needed for testing in chado
	LoadGpadChadoFixtureSqlite(chado, b)

	return &chadoTest{
		rice:   b,
		chado:  chado,
		parser: gochado.NewSqlParserFromString(str),
	}
}
