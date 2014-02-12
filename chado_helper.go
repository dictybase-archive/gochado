package gochado

import (
    "github.com/jmoiron/sqlx"
    "strings"
    "sync"
)

// Type to hold an active chado database handler. It is expected to be embedded in
// other type structure that requires an active chado connection.
type Database struct {
    ChadoHandler *sqlx.DB
}

// A simple thread safe cache for holding key(string) value(int). This is a
// typical use case for working with chado database where db,dbxref, cv
// and cvterm entries are shared as foreign keys between most of the tables.
// Caching those foreign keys with a unique name reduces redundant database
// lookups.
type DataCache struct {
    cache map[string]int
    sync.RWMutex
}

// New instance of Datacache
func NewDataCache() *DataCache {
    return &DataCache{cache: make(map[string]int)}
}

// Add an entry to the cache
func (dc *DataCache) Set(key string, id int) {
    dc.Lock()
    defer dc.Unlock()
    dc.cache[key] = id
}

// Remove an entry
func (dc *DataCache) Remove(key string) {
    dc.Lock()
    defer dc.Unlock()
    delete(dc.cache, key)
}

// Check for the presence of an entry
func (dc *DataCache) Has(key string) bool {
    dc.RLock()
    defer dc.RUnlock()
    if _, ok := dc.cache[key]; ok {
        return true
    }
    return false
}

// Retrieve an entry from the cache. However, it is advisable to run *Has*
// method before retrieving the entry.
func (dc *DataCache) Get(key string) (id int) {
    dc.RLock()
    defer dc.RUnlock()
    if v, ok := dc.cache[key]; ok {
        id = v
    }
    return
}

// Removes all entries from cache
func (dc *DataCache) Clear() {
    dc.Lock()
    defer dc.Unlock()
    dc.cache = make(map[string]int)
}

// Helper for finding and creating cv, cvterm , db and dbxrefs in chado
// database.
type ChadoHelper struct {
    *Database
    caches map[string]*DataCache
}

// Gets a new instance
func NewChadoHelper(dbh *sqlx.DB) *ChadoHelper {
    m := make(map[string]*DataCache)
    for _, name := range []string{"db", "cv", "cvterm", "dbxref"} {
        m[name] = &DataCache{}
    }
    return &ChadoHelper{&Database{ChadoHandler: dbh}, m}
}

// Given a db name returns its primary key identifier. The lookup is done on
// the cache first and if absent retrieved from db table.
func (helper *ChadoHelper) FindOrCreateDbId(db string) (int, error) {
    dbcache := helper.caches["db"]
    if dbcache.Has(db) {
        return dbcache.Get(db), nil
    }
    sqlx := helper.Database.ChadoHandler
    q := "SELECT db_id FROM db WHERE name = $1"
    row := sqlx.QueryRowx(q, db)
    var dbid int
    err := row.Scan(&dbid)
    if err != nil {
        return 0, err
    }
    if dbid != 0 {
        dbcache.Set(db, dbid)
        return dbid, nil
    }

    tx := sqlx.MustBegin()
    result := tx.Execl("INSERT INTO db(name) VALUES($1)", db)
    err = tx.Commit()
    if err != nil {
        return 0, err
    }
    id64, err := result.LastInsertId()
    if err != nil {
        return 0, err
    }
    id := int(id64)
    dbcache.Set(db, id)
    return id, nil
}

// Given a cv namespace returns its primary key identifier. The lookup is done on
// the cache first and if absent retrieved from cv table.
func (helper *ChadoHelper) FindOrCreateCvId(cv string) (int, error) {
    cvcache := helper.caches["cv"]
    if cvcache.Has(cv) {
        return cvcache.Get(cv), nil
    }
    sqlx := helper.Database.ChadoHandler
    q := "SELECT cv_id FROM cv WHERE name = $1"
    row := sqlx.QueryRowx(q, cv)
    var cvid int
    err := row.Scan(&cvid)
    if err != nil {
        return 0, err
    }
    if cvid != 0 {
        cvcache.Set(cv, cvid)
        return cvid, nil
    }

    tx := sqlx.MustBegin()
    result := tx.Execl("INSERT INTO cv(name) VALUES($1)", cv)
    id64, err := result.LastInsertId()
    if err != nil {
        _ = tx.Rollback()
        return 0, err
    }
    err = tx.Commit()
    if err != nil {
        _ = tx.Rollback()
        return 0, err
    }
    id := int(id64)
    cvcache.Set(cv, id)
    return id, nil
}

// Given a cvterm, cv and db names returns primary key of cvterm
// table(cvterm_id). The lookup is done on
// the cache first and if absent retrieved from cvterm table.
func (helper *ChadoHelper) FindOrCreateCvtermId(cv, cvt, db string) (int, error) {
    cvtcache := helper.caches["cvterm"]
    cvterm := cv + "-" + cvt
    if cvtcache.Has(cvterm) {
        return cvtcache.Get(cvterm), nil
    }
    sqlx := helper.Database.ChadoHandler
    q := `
    SELECT cvterm_id FROM cvterm JOIN cv ON cv.cv_id = cvterm.cv_id
    WHERE cv.name = $1 AND cvterm.name = $1
    `
    row := sqlx.QueryRowx(q, cv, cvt)
    var cvtid int
    err := row.Scan(&cvtid)
    if err != nil {
        return 0, err
    }
    if cvtid != 0 {
        cvtcache.Set(cvterm, cvtid)
        return cvtid, nil
    }

    //create cvterm
    dbid, err := helper.FindOrCreateDbId(db)
    if err != nil {
        return 0, err
    }
    cvid, err := helper.FindOrCreateCvId(cv)
    if err != nil {
        return 0, err
    }
    tx := sqlx.MustBegin()
    result := tx.Execl("INSERT INTO dbxref(db_id,accession) VALUES($1, $2)", dbid, cvt)
    dbxrefid, err := result.LastInsertId()
    if err != nil {
        _ = tx.Rollback()
        return 0, err
    }

    result = tx.Execl("INSERT INTO cvterm(cv_id,name,dbxref_id) VALUES($1, $2,$3)", cvid, cvt, dbxrefid)
    id64, err := result.LastInsertId()
    if err != nil {
        _ = tx.Rollback()
        return 0, err
    }
    err = tx.Commit()
    if err != nil {
        _ = tx.Rollback()
        return 0, err
    }
    id := int(id64)
    cvtcache.Set(cvterm, id)
    return id, nil
}

// Given a dbxref returns its db_id and accession. Accepts both Db:Dbxref and
// Dbxref form.
func (helper *ChadoHelper) NormaLizeId(dbxref string) (int, string, error) {
    if strings.Contains(dbxref, ":") {
        xrefs := strings.SplitN(dbxref, ":", 1)
        dbid, err := helper.FindOrCreateDbId(xrefs[0])
        if err != nil {
            return 0, "", err
        }
        return dbid, xrefs[1], nil
    }
    dbid, err := helper.FindOrCreateDbId(dbxref)
    if err != nil {
        return 0, "", err
    }
    return dbid, dbxref, nil
}
