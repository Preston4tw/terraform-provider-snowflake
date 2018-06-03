package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

func resourceSnowflakeView() *schema.Resource {
	return &schema.Resource{
		Create: resourceSnowflakeViewCreate,
		Read:   resourceSnowflakeViewRead,
		// Update: resourceSnowflakeViewUpdate,
		Delete: resourceSnowflakeViewDelete,
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
				ForceNew: true,
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
			"view_definition": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"comment": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
			},
			"secure": {
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
			},
		},
	}
}

func resourceSnowflakeViewCreate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	database := d.Get("database")
	schema := d.Get("schema")
	name := d.Get("name")
	viewID := strings.ToUpper(fmt.Sprintf("%s.%s.%s", database, schema, name))
	viewDefinition := d.Get("view_definition").(string)
	vdl := strings.ToLower(viewDefinition)
	if !(strings.HasPrefix(vdl, "create view") || strings.HasPrefix(vdl, "create or replace view")) {
		return fmt.Errorf("view_definition must begin with 'create view' or 'create or replace view'")
	}
	if !(strings.HasPrefix(vdl, fmt.Sprintf("create view %s as", strings.ToLower(viewID))) || strings.HasPrefix(vdl, fmt.Sprintf("create or replace view %s as", strings.ToLower(viewID)))) {
		return fmt.Errorf("view_definition destination does not match resource parameters: %s", viewID)
	}
	// strings.HasPrefix
	statement := fmt.Sprintf("%s", viewDefinition)
	_, err := db.Exec(statement)
	if err != nil {
		return err
	}
	d.SetId(strings.ToUpper(viewID))
	return nil
}

func resourceSnowflakeViewRead(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	s := strings.Split(d.Id(), ".")
	database, schema, name := s[0], s[1], s[2]
	t, err := readView(db, database, schema, name)
	if err != nil {
		return err
	}
	d.Set("name", t.tableName)
	d.Set("database", t.tableCatalog)
	d.Set("schema", t.tableSchema)
	d.Set("view_definition", t.viewDefinition)
	d.Set("comment", t.comment)
	d.Set("secure", t.isSecure == "YES")
	return nil
}

// func resourceSnowflakeViewUpdate(d *schema.ResourceData, meta interface{}) error {
// 	db := meta.(*sql.DB)
// 	s := strings.Split(d.Id(), ".")
// 	databaseName, schemaName, viewName := s[0], s[1], s[2]
// 	// Rather than issue a single alter database statement for all possible
// 	// changes issue an alter for each possible thing that has changed. Enable
// 	// partial mode.
// 	d.Partial(true)
// 	if d.HasChange("name") {
// 		// check that the rename target does not exist
// 		exists, err := sqlObjExists(db, "views", d.Get("name").(string), fmt.Sprintf("%s.%s", databaseName, schemaName))
// 		if err != nil {
// 			return err
// 		}
// 		if exists == true {
// 			return fmt.Errorf("Cannot rename %s to %s.%s.%s, already exists", d.Id(), databaseName, schemaName, viewName)
// 		}
// 		statement := fmt.Sprintf("ALTER TABLE %s RENAME TO %s.%s.%s", d.Id(), databaseName, schemaName, d.Get("name"))
// 		if _, err := db.Exec(statement); err != nil {
// 			return err
// 		}
// 		d.SetPartial("name")
// 		newResourceID := fmt.Sprintf("%s.%s.%s", databaseName, schemaName, d.Get("name"))
// 		d.SetId(newResourceID)
// 	}
// 	d.Partial(false)
// 	return nil
// }

func resourceSnowflakeViewDelete(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	s := strings.Split(d.Id(), ".")
	database, schema, name := s[0], s[1], s[2]
	exists, err := sqlObjExists(db, "views", name, fmt.Sprintf("%s.%s", database, schema))
	if err != nil {
		return err
	}
	if exists == false {
		return fmt.Errorf("View %s does not exist", d.Id())
	}
	statement := fmt.Sprintf("DROP VIEW %s", d.Id())
	if _, err := db.Exec(statement); err != nil {
		return err
	}
	return nil
}
