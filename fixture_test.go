package gochado

import (
	"encoding/gob"
	"log"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/GeertJohan/go.rice"
	"github.com/dictybase/testchado"
	. "github.com/onsi/gomega"
)

func getDataDir() string {
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		log.Fatal("unable to locate the data dir")
	}
	return filepath.Join(filepath.Dir(file), "data")
}

func TestGpadFixtureLoader(t *testing.T) {
	RegisterTestingT(t)
	chado := testchado.NewDBManager()
	chado.DeploySchema()
	_ = chado.LoadPresetFixture("eco")
	defer chado.DropSchema()

	//get the gob file
	b := rice.MustFindBox(getDataDir())
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
	f := NewGpadFixtureLoader(chado)
	g := f.LoadGenes(genes)
	Expect(g).Should(HaveLen(7), "expected 7 genes")

	var gorefs []string
	err = dec.Decode(&gorefs)
	if err != nil {
		log.Fatal(err)
	}
	p := f.LoadPubIds(gorefs)
	Expect(p).Should(HaveLen(4), "expected 4 pubs")

	var goids map[string][]string
	err = dec.Decode(&goids)
	if err != nil {
		log.Fatal(err)
	}
	goterm := f.LoadGoIds(goids)
	Expect(goterm).Should(HaveLen(9), "expected 9 goterms")

	gorm := f.gorm
	dbxref := Dbxref{}
	db := Db{}
	for _, cvterm := range goterm {
		gorm.Model(&cvterm).Related(&dbxref)
		gorm.First(&db, dbxref.DbId)
		Expect(db.Name).Should(Equal("GO"), "Expected GO")
	}

	mterm := f.LoadMiscCvterms("gene_ontology_association")
	Expect(mterm).Should(HaveLen(4), "expected 4 misc terms")
}
