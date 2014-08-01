package gochado

import (
	"encoding/gob"
	"testing"

	"github.com/GeertJohan/go.rice"
	"github.com/dictybase/testchado"
)

func TestGpadFixtureLoader(t *testing.T) {
	chado := testchado.NewDBManager()
	chado.DeploySchema()
	_ = chado.LoadPresetFixture("eco")
	defer chado.DropSchema()

	//get the gob file
	b := rice.MustFindBox("data")
	r, err := b.Open("fixture.gob")
	defer r.Close()
	if err != nil {
		t.Error("Could not get gob file fixture.gob")
	}
	// Now decode and get the data
	dec := gob.NewDecoder(r)
	var genes []string
	err = dec.Decode(&genes)
	if err != nil {
		t.Error(err)
	}
	f := NewGpadFixtureLoader(chado)
	g := f.LoadGenes(genes)
	if len(g) != 5 {
		t.Errorf("expected %d genes got %d", 5, len(g))
	}

	var gorefs []string
	err = dec.Decode(&gorefs)
	if err != nil {
		t.Error(err)
	}
	p := f.LoadPubIds(gorefs)
	if len(p) != 3 {
		t.Errorf("expected %d pubs got %d", 3, len(p))
	}

	var goids map[string][]string
	err = dec.Decode(&goids)
	if err != nil {
		t.Error(err)
	}
	goterm := f.LoadGoIds(goids)
	if len(goterm) != 8 {
		t.Errorf("expected %d go terms got %d", 8, len(goterm))
	}
	gorm := f.gorm
	dbxref := Dbxref{}
	db := Db{}
	for _, cvterm := range goterm {
		gorm.Model(&cvterm).Related(&dbxref)
		gorm.First(&db, dbxref.DbId)
		if db.Name != "GO" {
			t.Errorf("expect GO got %s", db.Name)
		}
	}

	mterm := f.LoadMiscCvterms("gene_ontology_association")
	if len(mterm) != 4 {
		t.Errorf("expected %d misc terms got %d", 4, len(mterm))
	}

}
