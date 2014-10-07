package gochado

// Interface for making a loader for staging tables
type StagingLoader interface {
	// Add a row of unprocessed data to the staging cache
	AddDataRow(string)
	// Create temporary staging tables
	CreateTables()
	// Drop the staging tables
	DropTables()
	// Alteration in the staging tables, for example creating indexes
	AlterTables()
	// Bulk upload data from staging cache to the staging tables
	BulkLoad()
	// Run before bulk upload to staging tables for accommodating
	// anything that's needed before bulk loading
	PreLoad()
	// Run after bulk upload to staging tables
	PostLoad()
	// Primarilly to complement the AlterTables method, put back the chaged tables
	// in its pristine states. It might be used to prune all rows to get it ready
	// for another cycle of loading
	ResetTables()
}
