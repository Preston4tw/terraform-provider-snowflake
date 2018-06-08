package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

func resourceSnowflakePipe() *schema.Resource {
	return &schema.Resource{
		Create: resourceSnowflakePipeCreate,
		Read:   resourceSnowflakePipeRead,
		Update: resourceSnowflakePipeUpdate,
		Delete: resourceSnowflakePipeDelete,
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
			"schema": {
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
			"comment": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"copy_statement": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				StateFunc: func(v interface{}) string {
					return strings.TrimSpace(v.(string))
				},
			},
			"auto_ingest": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
				ForceNew: true,
			},
			"notification_channel": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"owner": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

func resourceSnowflakePipeCreate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	databaseName := d.Get("database")
	schemaName := d.Get("schema")
	name := d.Get("name")
	comment := d.Get("comment")
	copyStatement := d.Get("copy_statement")
	autoIngest := d.Get("auto_ingest")
	pipeID := fmt.Sprintf("%s.%s.%s", databaseName, schemaName, name)
	statement := fmt.Sprintf("CREATE PIPE %s auto_ingest=%t comment='%s' as %s", pipeID, autoIngest, comment, copyStatement)
	_, err := db.Exec(statement)
	if err != nil {
		return err
	}
	d.SetId(strings.ToUpper(pipeID))
	return nil
}
func resourceSnowflakePipeRead(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	pipeID := d.Id()
	s := strings.Split(pipeID, ".")
	database, schema, name := s[0], s[1], s[2]
	r, err := showPipe(db, database, schema, name)
	if err != nil {
		return err
	}
	d.Set("database", r.databaseName)
	d.Set("schema", r.schemaName)
	d.Set("copy_statement", strings.TrimSpace(r.definition))
	d.Set("owner", r.owner)
	d.Set("notification_channel", r.notificationChannel)
	d.Set("auto_ingest", r.notificationChannel != "")
	d.Set("comment", r.comment)
	d.Set("name", r.name)

	return nil
}
func resourceSnowflakePipeUpdate(d *schema.ResourceData, meta interface{}) error {
	// Update comment
	return nil
}
func resourceSnowflakePipeDelete(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	s := strings.Split(d.Id(), ".")
	databaseName, schemaName, name := s[0], s[1], s[2]
	exists, err := sqlObjExists(db, "pipes", name, fmt.Sprintf("%s.%s", databaseName, schemaName))
	if err != nil {
		return err
	}
	if exists == false {
		return fmt.Errorf("Pipe %s does not exist", d.Id())
	}
	statement := fmt.Sprintf("DROP PIPE %s", d.Id())
	if _, err := db.Exec(statement); err != nil {
		return err
	}
	return nil
}
