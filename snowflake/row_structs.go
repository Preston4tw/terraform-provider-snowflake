package snowflake

import (
	"time"
)

type showDatabaseRow struct {
	createdOn     time.Time
	name          string
	isDefault     string
	isCurrent     string
	origin        string
	owner         string
	comment       string
	options       string
	retentionTime string
}

type showSchemaRow struct {
	createdOn     time.Time
	name          string
	isDefault     string
	isCurrent     string
	databaseName  string
	owner         string
	comment       string
	options       string
	retentionTime string
}
