package snowflake

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

var reViewPrefix = regexp.MustCompile(`(?i)^create (or replace )?view .* as\n`)

func resourceSnowflakeView() *schema.Resource {
	return &schema.Resource{
		Create: resourceSnowflakeViewCreate,
		Read:   resourceSnowflakeViewRead,
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
				StateFunc: func(v interface{}) string {
					viewDefinition := v.(string)
					createViewPos := reViewPrefix.FindStringIndex(viewDefinition)
					if createViewPos != nil {
						return viewDefinition[createViewPos[1]:]
					}
					return viewDefinition
				},
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
	statement := fmt.Sprintf("create view %s.%s.%s as\n%s", database, schema, name, viewDefinition)
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
	d.Set("comment", t.comment)
	d.Set("secure", t.isSecure == "YES")
	d.Set("view_definition", t.viewDefinition[reViewPrefix.FindStringIndex(t.viewDefinition)[1]:])
	return nil
}

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
