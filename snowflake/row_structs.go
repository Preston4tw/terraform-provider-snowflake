package snowflake

import (
	"database/sql"
	"time"
)

type showUserRow struct {
	name 					string
	created_on 				time.Time
	login_name				string
	display_name			string
	first_name				string
	last_name				string
	email					string
	mins_to_unlock			string
	days_to_expiry			string
	comment					string
	disabled				bool
	must_change_password	bool
	snowflake_lock			bool
	default_warehouse		string
	default_namespace		string
	default_role			string
	ext_authn_duo			string
	ext_authn_uid			string
	mins_to_bypass_mfa		string
	owner 					string
	last_success_login		sql.NullString
	expires_at_time			sql.NullString
	locked_until_time		sql.NullString
	has_password			bool
	has_rsa_public_key		bool
}

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

type showTableRow struct {
	createdOn     time.Time
	name          string
	databaseName  string
	schemaName    string
	kind          string
	comment       string
	clusterBy     string
	rows          int
	bytes         int
	owner         string
	retentionTime string
}

type descTableRow struct {
	colName      string
	colType      string
	kind         string
	isNullable   string
	defaultValue sql.NullString
	isPrimaryKey string
	isUniqueKey  string
	check        sql.NullString
	expression   sql.NullString
	comment      sql.NullString
}

type showColumnsRow struct {
	tableName     string
	schemaName    string
	columnName    string
	dataType      map[string]interface{}
	isNullable    string
	defaultValue  string
	kind          string
	expression    string
	comment       string
	databaseName  string
	autoincrement string
}

type showPipeRow struct {
	createdOn           time.Time
	name                string
	databaseName        string
	schemaName          string
	definition          string
	owner               string
	notificationChannel string
	comment             string
}

type infoSchemaDatabase struct {
	databaseName  string
	databaseOwner string
	isTransient   string
	comment       sql.NullString
	created       time.Time
	lastAltered   time.Time
	retentionTime int
}

type infoSchemaSchemata struct {
	catalogName                string
	schemaName                 string
	schemaOwner                string
	isTransient                string
	retentionTime              int
	defaultCharacterSetCatalog sql.NullString
	defaultCharacterSetSchema  sql.NullString
	defaultCharacterSetName    sql.NullString
	sqlPath                    sql.NullString
	created                    sql.NullString
	lastAltered                sql.NullString
	comment                    sql.NullString
}

type infoSchemaTable struct {
	tableCatalog              string
	tableSchema               string
	tableName                 string
	tableOwner                string
	tableType                 string
	isTransient               string
	clusteringKey             sql.NullString
	rowCount                  int
	bytes                     int
	retentionTime             int
	selfReferencingColumnName sql.NullString
	referenceGeneration       sql.NullString
	userDefinedTypeColumn     sql.NullString
	userDefinedTypeSchema     sql.NullString
	userDefinedTypeName       sql.NullString
	isInsertableInto          string
	isTyped                   string
	commitAction              sql.NullString
	created                   time.Time
	lastAltered               time.Time
	comment                   sql.NullString
}

type infoSchemaView struct {
	tableCatalog   string
	tableSchema    string
	tableName      string
	tableOwner     string
	viewDefinition string
	checkOption    string
	isUpdatable    string
	insertableInto string
	isSecure       string
	created        time.Time
	lastAltered    time.Time
	comment        sql.NullString
}

type infoSchemaColumn struct {
	tableCatalog           string
	tableSchema            string
	tableName              string
	columnName             string
	ordinalPosition        int
	columnDefault          sql.NullString
	isNullable             string
	dataType               string
	characterMaximumLength sql.NullInt64
	characterOctetLength   sql.NullInt64
	numericPrecision       sql.NullInt64
	numericPrecisionRadix  sql.NullInt64
	numericScale           sql.NullInt64
	datetimePrecision      sql.NullInt64
	intervalType           sql.NullString
	intervalPrecision      sql.NullString
	characterSetCatalog    sql.NullString
	characterSetSchema     sql.NullString
	characterSetName       sql.NullString
	collationCatalog       sql.NullString
	collationSchema        sql.NullString
	collationName          sql.NullString
	domainCatalog          sql.NullString
	domainSchema           sql.NullString
	domainName             sql.NullString
	udtCatalog             sql.NullString
	udtSchema              sql.NullString
	udtName                sql.NullString
	scopeCatalog           sql.NullString
	scopeSchema            sql.NullString
	scopeName              sql.NullString
	maximumCardinality     sql.NullString
	dtdIdentifier          sql.NullString
	isSelfReferencing      string
	isIdentity             string
	identityGeneration     sql.NullString
	identityStart          sql.NullString
	identityIncrement      sql.NullString
	identityMaximum        sql.NullString
	identityMinimum        sql.NullString
	identityCycle          sql.NullString
	comment                sql.NullString
}
