package gochado

import (
	"encoding/gob"
	"log"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/GeertJohan/go.rice"
	. "github.com/onsi/gomega"
	"gopkg.in/dictybase/testchado.v1"
	. "gopkg.in/dictybase/testchado.v1/matchers"
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
	b := rice.MustFindBox("data")
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
	goterms := f.LoadGoIds(goids)
	Expect(goterms).Should(HaveLen(10), "expected 10 goterms")

	gorm := f.gorm
	dbxref := Dbxref{}
	db := Db{}
	for _, cvterm := range goterms {
		gorm.Model(&cvterm).Related(&dbxref)
		gorm.First(&db, dbxref.DbId)
		Expect(db.Name).Should(Equal("GO"), "Expected GO")
	}

	mterms := f.LoadMiscCvterms("gene_ontology_association")
	Expect(mterms).Should(HaveLen(4), "expected 4 misc terms")

	// Anonymous namespaces
	f.LoadAnonNamespaces()
	cvs := []string{
		"annotation extension terms",
		"go/extensions/gorel",
	}
	for _, c := range cvs {
		Expect(chado).Should(HaveCv(c))
	}
	// Extension cvterms
	var cvtslice []map[string]string
	err = dec.Decode(&cvtslice)
	if err != nil {
		log.Fatal(err)
	}
	Expect(cvtslice).Should(HaveLen(3))
	exterms := f.LoadExtnCvterms(cvtslice)
	Expect(exterms).Should(HaveLen(3))
	for i, cvt := range exterms {
		Expect(cvt.Name).Should(Equal(cvtslice[i]["cvterm"]))
	}

	// Dbxrefs
	var xrefs []string
	err = dec.Decode(&xrefs)
	if err != nil {
		log.Fatalf("decoding err in xref %s", err)
	}
	Expect(xrefs).Should(HaveLen(2))
	dbxrefs := f.LoadDbxrefs(xrefs)
	Expect(dbxrefs).Should(HaveLen(2))
}
