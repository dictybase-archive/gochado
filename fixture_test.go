package gochado

import (
    "github.com/dictybase/testchado"
    "testing"
)

var genes = []string{
    "DDB_G0272003",
    "DDB_G0272004",
    "DDB_G0271142",
    "DDB_G0292036",
    "DDB_G0278727",
}
var gorefs = []string{
    "GO_REF:0000002",
    "GO_REF:0000033",
    "GO_REF:0000037",
}

var goids = map[string]string{
    "GO:0001614": "purinergic nucleotide receptor activity",
    "GO:0006971": "hypotonic response",
    "GO:0003779": "actin binding",
    "GO:0005938": "cell cortex",
    "GO:0005615": "extracellular space",
    "GO:0005829": "cytosol",
    "GO:0015629": "actin cytoskeleton",
    "GO:0031152": "aggregation involved in sorocarp development",
}

func TestGpadFixtureLoader(t *testing.T) {
    chado := testchado.NewDBManager()
    chado.DeploySchema()
    _ = chado.LoadPresetFixture("eco")
    defer chado.DropSchema()
    f := NewGpadFixtureLoader(chado)
    g := f.LoadGenes(genes)
    if len(g) != 5 {
        t.Errorf("expected %d genes got %d", 5, len(g))
    }
    goterm := f.LoadGoIds(goids)
    if len(goterm) != 8 {
        t.Errorf("expected %d go terms got %d", 8, len(goterm))
    }
    p := f.LoadPubIds(gorefs)
    if len(p) != 3 {
        t.Errorf("expected %d pubs got %d", 3, len(p))
    }
    mterm := f.LoadMiscCvterms("gene_ontology_association")
    if len(mterm) != 4 {
        t.Errorf("expected %d misc terms got %d", 4, len(mterm))
    }

}
