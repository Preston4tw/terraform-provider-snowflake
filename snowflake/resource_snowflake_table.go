package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

func resourceSnowflakeTable() *schema.Resource {
	return &schema.Resource{
		Create: resourceSnowflakeTableCreate,
		Read:   resourceSnowflakeTableRead,
		Update: resourceSnowflakeTableUpdate,
		Delete: resourceSnowflakeTableDelete,
		Importer: &schema.ResourceImporter{
			State: func(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
				d.SetId(strings.ToUpper(d.Id()))
				return []*schema.ResourceData{d}, nil
			},
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
			},
			"database": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
			},
			"schema": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
			},
			"columns": {
				Type:     schema.TypeList,
				Required: true,
				ForceNew: true,
				// BUG: the values are not upper cased in the state as part of
				// create but will be during a refresh
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:     schema.TypeString,
							Required: true,
							StateFunc: func(v interface{}) string {
								return strings.ToUpper(v.(string))
							},
						},
						"type": {
							Type:     schema.TypeString,
							Required: true,
							StateFunc: func(v interface{}) string {
								return strings.ToUpper(v.(string))
							},
						},
					},
				},
			},
		},
	}
}

func resourceSnowflakeTableCreate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	databaseName := d.Get("database")
	schemaName := d.Get("schema")
	tableName := d.Get("name")
	tableID := fmt.Sprintf("%s.%s.%s", databaseName, schemaName, tableName)
	columnDefs := ""
	// This is black magic to me but it seems to work.
	// Casting d.Get("columns").([]map[string]interface{}) did not seem to work
	// but this two step approach seems to..
	for _, iElement := range d.Get("columns").([]interface{}) {
		element := iElement.(map[string]interface{})
		columnDefs += fmt.Sprintf("%s %s,", element["name"], element["type"])
	}
	columnDefs = strings.TrimRight(columnDefs, ",")
	statement := fmt.Sprintf("CREATE TABLE %s ( %s )", tableID, columnDefs)
	_, err := db.Exec(statement)
	if err != nil {
		return err
	}
	d.SetId(strings.ToUpper(tableID))
	return nil
}

func resourceSnowflakeTableRead(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	s := strings.Split(d.Id(), ".")
	database, schema, name := s[0], s[1], s[2]
	t, err := readTable(db, database, schema, name)
	if err != nil {
		return err
	}
	d.Set("name", t.tableName)
	d.Set("database", t.tableCatalog)
	d.Set("schema", t.tableSchema)
	columnDefs := []map[string]string{}
	columnInfo, err := descTable(db, database, schema, name)
	for _, e := range columnInfo {
		columnDefs = append(columnDefs, map[string]string{
			"name": e.colName,
			"type": e.colType,
		})
	}
	d.Set("columns", columnDefs)
	return nil
}

func resourceSnowflakeTableUpdate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	s := strings.Split(d.Id(), ".")
	databaseName, schemaName, tableName := s[0], s[1], s[2]
	// Rather than issue a single alter database statement for all possible
	// changes issue an alter for each possible thing that has changed. Enable
	// partial mode.
	d.Partial(true)
	if d.HasChange("name") {
		// check that the rename target does not exist
		exists, err := sqlObjExists(db, "tables", d.Get("name").(string), fmt.Sprintf("%s.%s", databaseName, schemaName))
		if err != nil {
			return err
		}
		if exists == true {
			return fmt.Errorf("Cannot rename %s to %s.%s.%s, already exists", d.Id(), databaseName, schemaName, tableName)
		}
		statement := fmt.Sprintf("ALTER TABLE %s RENAME TO %s.%s.%s", d.Id(), databaseName, schemaName, d.Get("name"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("name")
		newResourceID := fmt.Sprintf("%s.%s.%s", databaseName, schemaName, d.Get("name"))
		d.SetId(newResourceID)
	}
	d.Partial(false)
	return nil
}

func resourceSnowflakeTableDelete(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	s := strings.Split(d.Id(), ".")
	databaseName, schemaName, name := s[0], s[1], s[2]
	exists, err := sqlObjExists(db, "tables", name, fmt.Sprintf("%s.%s", databaseName, schemaName))
	if err != nil {
		return err
	}
	if exists == false {
		return fmt.Errorf("Table %s does not exist", d.Id())
	}
	statement := fmt.Sprintf("DROP TABLE %s", d.Id())
	if _, err := db.Exec(statement); err != nil {
		return err
	}
	return nil
}
