package gochado

import (
    "bytes"
    "crypto/md5"
    "encoding/hex"
    "fmt"
    "github.com/jmoiron/sqlx"
    "io"
    "io/ioutil"
    "log"
    "strings"
    "sync"
)

// Returns MD5 hash of string
func GetMD5Hash(text string) string {
    hasher := md5.New()
    hasher.Write([]byte(text))
    return hex.EncodeToString(hasher.Sum(nil))
}

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
        m[name] = NewDataCache()
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
    _ = row.Scan(&dbid)
    if dbid != 0 {
        dbcache.Set(db, dbid)
        return dbid, nil
    }

    tx := sqlx.MustBegin()
    result := tx.Execl("INSERT INTO db(name) VALUES($1)", db)
    id64, err := result.LastInsertId()
    if err != nil {
        _ = tx.Rollback()
        return 0, fmt.Errorf("error %s in retreiving db_id", err)
    }
    err = tx.Commit()
    if err != nil {
        _ = tx.Rollback()
        return 0, fmt.Errorf("error %s in commiting record ", err)
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
    _ = row.Scan(&cvid)
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

func (helper *ChadoHelper) FindCvtermId(cv, cvt string) (int, error) {
    cvtcache := helper.caches["cvterm"]
    cvterm := cv + "-" + cvt
    if cvtcache.Has(cvterm) {
        return cvtcache.Get(cvterm), nil
    }
    sqlx := helper.Database.ChadoHandler
    q := `
    SELECT cvterm_id FROM cvterm JOIN cv ON cv.cv_id = cvterm.cv_id
    WHERE cv.name = $1 AND cvterm.name = $2
    `
    row := sqlx.QueryRowx(q, cv, cvt)
    var cvtid int
    err := row.Scan(&cvtid)
    if err != nil {
        return 0, err
    }
    cvtcache.Set(cvterm, cvtid)
    return cvtid, nil
}

// Creates a cvterm record from cvterm, cv, dbxref and db parameters and
// return a primary key of cvterm
// table(cvterm_id). The parameters are passed through a map structure with
// the following keys
//  cv :    manadatory
//  cvterm: manadatory
//  dbxref: mandatory. If dbxref has Db:Id structure the Db and Id are split before
//          storing
//  db:     optional. By default internal is used.
func (helper *ChadoHelper) CreateCvtermId(params map[string]string) (int, error) {
    for _, k := range []string{"cv", "cvterm", "dbxref"} {
        if _, ok := params[k]; !ok {
            return 0, fmt.Errorf("missing key %s", k)
        }
    }
    var db string
    var xref string
    if strings.Contains(params["dbxref"], ":") {
        d := strings.SplitN(params["dbxref"], ":", 2)
        db = d[0]
        xref = d[1]

    } else {
        xref = params["dbxref"]
        db = "internal"
    }
    if v, ok := params["db"]; ok {
        db = v
    }
    sqlx := helper.Database.ChadoHandler
    //create cvterm
    dbid, err := helper.FindOrCreateDbId(db)
    if err != nil {
        return 0, fmt.Errorf("error %s with FindOrCreateDbId()", err)
    }
    cvid, err := helper.FindOrCreateCvId(params["cv"])
    if err != nil {
        return 0, fmt.Errorf("error %s with FindOrCreateCvId()", err)
    }
    tx := sqlx.MustBegin()
    result := tx.Execl("INSERT INTO dbxref(db_id,accession) VALUES($1, $2)", dbid, xref)
    dbxrefid, err := result.LastInsertId()
    if err != nil {
        _ = tx.Rollback()
        return 0, fmt.Errorf("error %s with retreiving dbid", err)
    }

    result = tx.Execl("INSERT INTO cvterm(cv_id,name,dbxref_id) VALUES($1, $2,$3)", cvid, params["cvterm"], dbxrefid)
    id64, err := result.LastInsertId()
    if err != nil {
        _ = tx.Rollback()
        return 0, fmt.Errorf("error %s with retreiving cvtermid", err)
    }
    err = tx.Commit()
    if err != nil {
        _ = tx.Rollback()
        return 0, fmt.Errorf("error %s with commiting", err)
    }
    id := int(id64)
    cvtcache := helper.caches["cvterm"]
    cvtcache.Set(params["cv"]+"-"+params["cvterm"], id)
    return id, nil
}

// Given a dbxref returns its db_id and accession. Accepts both Db:Dbxref and
// Dbxref form. Create the Db entry if absent in the database. In case of
// without specific Db, *internal* is used
func (helper *ChadoHelper) NormaLizeId(dbxref string) (int, string, error) {
    if strings.Contains(dbxref, ":") {
        xrefs := strings.SplitN(dbxref, ":", 2)
        dbid, err := helper.FindOrCreateDbId(xrefs[0])
        if err != nil {
            return 0, "", fmt.Errorf("error %s with Db:Dbxref form of dbxref", err)
        }
        return dbid, xrefs[1], nil
    }
    dbid, err := helper.FindOrCreateDbId("internal")
    if err != nil {
        return 0, "", fmt.Errorf("error %s with Dbxref form of dbxref", err)
    }
    return dbid, dbxref, nil
}

// Parsing sql statements from ini style config file. Each ini section expects
// to have a sql statement
/*
In caboose.ini file

[create_bag]
CREATE TABLE bag (
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT
);

[select_bag]
SELECT id FROM bag WHERE name = ?

[insert_bag]
INSERT INTO bag(name) VALUES(?)

.......


   parser := NewSqlParserFromFile("caboose.ini")
   for _, section := range parser.Sections() {
       fmt.Printf("section:%s\nvalue:%s\n\n",section,parser.GetSection(section))
   }

*/
type SqlParser struct {
    content map[string]string
}

// Parse ini sql content from a string and returns a new instance
func NewSqlParserFromString(content string) *SqlParser {
    buffer := bytes.NewBufferString(content)
    return &SqlParser{content: ParseConfig(buffer)}
}

// Parse ini sql content from a file and returns a new instance
func NewSqlParserFromFile(file string) *SqlParser {
    c, err := ioutil.ReadFile(file)
    if err != nil {
        log.Fatal(err)
    }
    buffer := bytes.NewBuffer(c)
    return &SqlParser{content: ParseConfig(buffer)}

}

// Parse ini content from a buffer
func ParseConfig(buffer *bytes.Buffer) map[string]string {
    var curr string
    var b bytes.Buffer
    content := make(map[string]string)

    for {
        line, err := buffer.ReadString('\n')
        if err == io.EOF {
            content[curr] = b.String() + line
            break
        }
        if strings.HasPrefix(line, "\n") {
            continue
        }
        // skip comment
        if strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
            continue
        }
        if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]\n") {
            key := line[1 : len(line)-2]
            if _, ok := content[key]; ok {
                log.Fatal("duplicate keys not allowed")
            } else {
                if len(curr) == 0 { //first block
                    curr = key
                } else {
                    content[curr] = b.String()
                    curr = key
                    b.Reset()
                }
            }
        } else {
            b.WriteString(line)
        }
    }
    return content
}

// List of ini section
func (ini *SqlParser) Sections() []string {
    var s []string
    for k, _ := range ini.content {
        s = append(s, k)
    }
    return s
}

// Value of a particular section
func (ini *SqlParser) GetSection(key string) string {
    if _, ok := ini.content[key]; ok {
        return ini.content[key]
    }
    return ""
}

// A simple way to hold bucket of data primarilly for inserting in batch to a relational backend.
// It is a simple slice of maps container where each will have values keyed by
// a column name. Ultimately, each of this map will be transformed into a row
// in the database.
type DataBucket struct {
    bucket []map[string]string
}

func NewDataBucket() *DataBucket {
    return &DataBucket{bucket: make([]map[string]string, 0)}
}

func (b *DataBucket) Push(m map[string]string) {
    b.bucket = append(b.bucket, m)
}

func (b *DataBucket) Clear() {
    b.bucket = make([]map[string]string, 0)
}

func (b *DataBucket) GetByPosition(pos int) map[string]string {
    return b.bucket[pos]
}

func (b *DataBucket) Elements() []map[string]string {
    return b.bucket
}

func (b *DataBucket) Count() int {
    return len(b.bucket)
}
