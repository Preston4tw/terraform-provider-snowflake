package snowflake

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
)

/*
showStatementExists is a helper function to check whether a Snowflake object
exists or not. In the context of the Snowflake terraform provider, before
modifying any object we want to know that one and only one object exists. While
by default identifiers are case insensitive, (see
https://docs.snowflake.net/manuals/sql-reference/identifiers-syntax.html)
identifiers can be made case sensitive by wrapping them in double quotes. For
instance, it's possible to issue these queries:

create database "foo";
create database "FOO";

And get two results for the following
show databases like 'foo';
*/
func showStatementExists(db *sql.DB, statement string) (bool, error) {
	rows, err := db.Query(statement)
	if err != nil {
		return true, err
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		index++
	}
	if index == 0 {
		return false, nil
	}
	if index == 1 {
		return true, nil
	}
	if index > 1 {
		return true, fmt.Errorf("More than 1 row returned for \"%s\"", statement)
	}
	return true, fmt.Errorf("this should never happen")
}

func sqlObjExists(db *sql.DB, objectType string, name string, inClause string) (bool, error) {
	statement := fmt.Sprintf("SHOW %s LIKE '%s' in %s", objectType, name, inClause)
	return showStatementExists(db, statement)
}

func showDatabase(db *sql.DB, name string) (showDatabaseRow, error) {
	var r showDatabaseRow
	// This verifies that one and only one database exists
	exists, err := sqlObjExists(db, "databases", name, "account")
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Database %s does not exist", name)
	}
	statement := fmt.Sprintf("SHOW DATABASES LIKE '%s'", name)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&r.createdOn,
			&r.name,
			&r.isDefault,
			&r.isCurrent,
			&r.origin,
			&r.owner,
			&r.comment,
			&r.options,
			&r.retentionTime,
		); err != nil {
			return r, err
		}
	}
	return r, nil
}

func showSchema(db *sql.DB, databaseName string, name string) (showSchemaRow, error) {
	var r showSchemaRow
	// This verifies that one and only one database exists
	exists, err := sqlObjExists(db, "schemas", name, databaseName)
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Schema %s.%s does not exist", databaseName, name)
	}
	statement := fmt.Sprintf("SHOW SCHEMAS LIKE '%s' in %s", name, databaseName)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&r.createdOn,
			&r.name,
			&r.isDefault,
			&r.isCurrent,
			&r.databaseName,
			&r.owner,
			&r.comment,
			&r.options,
			&r.retentionTime,
		); err != nil {
			return r, err
		}
	}
	return r, nil
}

func readTable(db *sql.DB, database string, schema string, name string) (infoSchemaTable, error) {
	var r infoSchemaTable
	exists, err := sqlObjExists(db, "tables", name, fmt.Sprintf("%s.%s", database, schema))
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Table %s.%s.%s does not exist", database, schema, name)
	}
	statement := fmt.Sprintf("SELECT * from %s.information_schema.tables where table_name = '%s' and table_schema = '%s'", database, name, schema)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&r.tableCatalog,
			&r.tableSchema,
			&r.tableName,
			&r.tableOwner,
			&r.tableType,
			&r.isTransient,
			&r.clusteringKey,
			&r.rowCount,
			&r.bytes,
			&r.retentionTime,
			&r.selfReferencingColumnName,
			&r.referenceGeneration,
			&r.userDefinedTypeColumn,
			&r.userDefinedTypeSchema,
			&r.userDefinedTypeName,
			&r.isInsertableInto,
			&r.isTyped,
			&r.commitAction,
			&r.created,
			&r.lastAltered,
			&r.comment,
		); err != nil {
			return r, err
		}
	}
	return r, nil
}

func readView(db *sql.DB, database string, schema string, name string) (infoSchemaView, error) {
	var r infoSchemaView
	exists, err := sqlObjExists(db, "views", name, fmt.Sprintf("%s.%s", database, schema))
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("View %s.%s.%s does not exist", database, schema, name)
	}
	statement := fmt.Sprintf("SELECT * from %s.information_schema.views where table_name = '%s' and table_schema = '%s'", database, name, schema)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&r.tableCatalog,
			&r.tableSchema,
			&r.tableName,
			&r.tableOwner,
			&r.viewDefinition,
			&r.checkOption,
			&r.isUpdatable,
			&r.insertableInto,
			&r.isSecure,
			&r.created,
			&r.lastAltered,
			&r.comment,
		); err != nil {
			return r, err
		}
	}
	return r, nil
}

func showTable(db *sql.DB, databaseName string, schemaName string, name string) (showTableRow, error) {
	var r showTableRow
	// This verifies that one and only one database exists
	exists, err := sqlObjExists(db, "tables", name, fmt.Sprintf("%s.%s", databaseName, schemaName))
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Table %s.%s.%s does not exist", databaseName, schemaName, name)
	}
	statement := fmt.Sprintf("SHOW TABLES LIKE '%s' in %s.%s", name, databaseName, schemaName)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&r.createdOn,
			&r.name,
			&r.databaseName,
			&r.schemaName,
			&r.kind,
			&r.comment,
			&r.clusterBy,
			&r.rows,
			&r.bytes,
			&r.owner,
			&r.retentionTime,
		); err != nil {
			return r, err
		}
	}
	return r, nil
}

func descTable(db *sql.DB, databaseName string, schemaName string, name string) ([]descTableRow, error) {
	var columnInfo []descTableRow
	// This verifies that one and only one database exists
	exists, err := sqlObjExists(db, "tables", name, fmt.Sprintf("%s.%s", databaseName, schemaName))
	if err != nil {
		return columnInfo, err
	}
	if exists == false {
		return columnInfo, fmt.Errorf("Table %s.%s.%s does not exist", databaseName, schemaName, name)
	}
	statement := fmt.Sprintf("DESC TABLE %s.%s.%s", databaseName, schemaName, name)
	rows, err := db.Query(statement)
	if err != nil {
		return columnInfo, err
	}
	defer rows.Close()
	for rows.Next() {
		var r descTableRow
		if err := rows.Scan(
			&r.colName,
			&r.colType,
			&r.kind,
			&r.isNullable,
			&r.defaultValue,
			&r.isPrimaryKey,
			&r.isUniqueKey,
			&r.check,
			&r.expression,
			&r.comment,
		); err != nil {
			return columnInfo, err
		}
		columnInfo = append(columnInfo, r)
	}
	return columnInfo, nil
}

func showPipe(db *sql.DB, database string, schema string, name string) (showPipeRow, error) {
	var r showPipeRow
	// This verifies that one and only one database exists
	exists, err := sqlObjExists(db, "pipes", name, fmt.Sprintf("%s.%s", database, schema))
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Pipe %s.%s.%s does not exist", database, schema, name)
	}
	statement := fmt.Sprintf("SHOW PIPES LIKE '%s' in %s.%s", name, database, schema)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(
			&r.createdOn,
			&r.name,
			&r.databaseName,
			&r.schemaName,
			&r.definition,
			&r.owner,
			&r.notificationChannel,
			&r.comment,
		); err != nil {
			return r, err
		}
	}
	return r, nil
}

func descUser(db *sql.DB, name string) (descUserResult, error) {
	var r descUserResult
	// This verifies that one and only one user exists
	exists, err := sqlObjExists(db, "users", name, "account")
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("User %s does not exist", name)
	}
	statement := fmt.Sprintf("DESC USER %s", name)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		var property string
		var value string
		var sfc_default string
		var description string
		if err := rows.Scan(&property, &value, &sfc_default, &description); err != nil {
			return r, err
		}
		switch property {
		case "NAME":
			r.name = value
		case "COMMENT":
			r.comment = value
		case "LOGIN_NAME":
			r.login_name = value
		case "DISPLAY_NAME":
			r.display_name = value
		case "FIRST_NAME":
			r.first_name = value
		case "MIDDLE_NAME":
			r.middle_name = value
		case "LAST_NAME":
			r.last_name = value
		case "EMAIL":
			r.email = value
		case "PASSWORD":
			r.password = value
		case "MUST_CHANGE_PASSWORD":
			r.must_change_password = value
		case "DISABLED":
			r.disabled = value
		case "SNOWFLAKE_LOCK":
			r.snowflake_lock = value
		case "SNOWFLAKE_SUPPORT":
			r.snowflake_support = value
		case "DAYS_TO_EXPIRY":
			r.days_to_expiry = value
		case "MINS_TO_UNLOCK":
			r.mins_to_unlock = value
		case "DEFAULT_WAREHOUSE":
			r.default_warehouse = value
		case "DEFAULT_NAMESPACE":
			r.default_namespace = value
		case "DEFAULT_ROLE":
			r.default_role = value
		case "EXT_AUTHN_DUO":
			r.ext_authn_duo = value
		case "EXT_AUTHN_UID":
			r.ext_authn_uid = value
		case "MINS_TO_BYPASS_MFA":
			r.mins_to_bypass_mfa = value
		case "MINS_TO_BYPASS_NETWORK_POLICY":
			r.mins_to_bypass_network_policy = value
		case "RSA_PUBLIC_KEY_FP":
			r.rsa_public_key = value
		case "RSA_PUBLIC_KEY_2_FP":
			r.rsa_public_key_2 = value
		}

	}
	return r, nil
}

func descStage(db *sql.DB, database string, schema string, name string) (descStageResult, error) {
	var r descStageResult
	exists, err := sqlObjExists(db, "stages", name, fmt.Sprintf("%s.%s", database, schema))
	if err != nil {
		return r, err
	}
	if exists == false {
		return r, fmt.Errorf("Stage %s does not exist", name)
	}
	statement := fmt.Sprintf("DESC STAGE %s.%s.%s", database, schema, name)
	rows, err := db.Query(statement)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	f, err := os.Create("/tmp/tflogs")
	defer f.Close()
	for rows.Next() {
		var parent_property string
		var property string
		var property_type string
		var property_value string
		var property_default string
		if err := rows.Scan(&parent_property, &property, &property_type, &property_value, &property_default); err != nil {
			return r, err
		}
		f.WriteString(fmt.Sprintf("%v : %v\n", property, property_value))

		switch property {
		case "URL":
			//when you DESC STAGE, the url is inside brackets and quotated. At least it's not in the middle of the other side, in parentheses and capital letters.
			r.url = strings.Trim(property_value, "[\"]")

		case "AWS_ROLE":
			r.aws_role = property_value

		case "AWS_EXTERNAL_ID":
			r.aws_external_id = property_value

		case "SNOWFLAKE_IAM_USER":
			f.WriteString("Setting r.snowflake_iam_user")
			r.snowflake_iam_user = property_value
		}
	}

	return r, nil
}
