package snowflake

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

func resourceSnowflakeSchema() *schema.Resource {
	return &schema.Resource{
		Create: resourceSnowflakeSchemaCreate,
		Read:   resourceSnowflakeSchemaRead,
		Update: resourceSnowflakeSchemaUpdate,
		Delete: resourceSnowflakeSchemaDelete,
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
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
			},
			"owner": {
				Type:     schema.TypeString,
				Computed: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
			},
			"comment": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"transient": {
				Type:     schema.TypeBool,
				Optional: true,
			},
			"retention_time": {
				Type:     schema.TypeInt,
				Default:  1,
				Optional: true,
			},
		},
	}
}

func resourceSnowflakeSchemaCreate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	resourceID := strings.ToUpper(fmt.Sprintf("%s.%s", d.Get("database"), d.Get("name")))
	statement := fmt.Sprintf("CREATE SCHEMA %s DATA_RETENTION_TIME_IN_DAYS = %d", resourceID, d.Get("retention_time"))
	if d.Get("transient").(bool) == true {
		statement = fmt.Sprintf("CREATE TRANSIENT SCHEMA %s DATA_RETENTION_TIME_IN_DAYS = %d", resourceID, d.Get("retention_time"))
		d.Set("transient", true)
	} else {
		d.Set("transient", false)
	}
	if d.Get("comment") != "" {
		statement += fmt.Sprintf(" COMMENT = '%s'", d.Get("comment"))
	}
	_, err := db.Exec(statement)
	if err != nil {
		return err
	}
	d.SetId(resourceID)
	return nil
}

func resourceSnowflakeSchemaRead(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	s := strings.Split(d.Id(), ".")
	database, schema := s[0], s[1]
	schemaInfo, err := showSchema(db, database, schema)
	if err != nil {
		return err
	}
	d.Set("name", schemaInfo.name)
	d.Set("database", schemaInfo.databaseName)
	d.Set("owner", schemaInfo.owner)
	d.Set("comment", schemaInfo.comment)
	if schemaInfo.options == "TRANSIENT" {
		d.Set("transient", true)
	} else {
		d.Set("transient", false)
	}
	if schemaInfo.retentionTime != "" {
		retentionTime, err := strconv.Atoi(schemaInfo.retentionTime)
		if err != nil {
			return err
		}
		d.Set("retention_time", retentionTime)
	}

	return nil
}

func resourceSnowflakeSchemaUpdate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	s := strings.Split(d.Id(), ".")
	database, name := s[0], s[1]
	// Rather than issue a single alter database statement for all possible
	// changes issue an alter for each possible thing that has changed. Enable
	// partial mode.
	d.Partial(true)
	if d.HasChange("name") {
		// check that the rename target does not exist
		exists, err := sqlObjExists(db, "schemas", name, database)
		if err != nil {
			return err
		}
		if exists == true {
			return fmt.Errorf("Cannot rename %s to %s.%s, %s.%s already exists", d.Id(), database, name, database, d.Get("name"))
		}
		statement := fmt.Sprintf("ALTER SCHEMA %s RENAME TO %s.%s", d.Id(), database, d.Get("name"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("name")
		newResourceID := fmt.Sprintf("%s.%s", database, d.Get("name"))
		d.SetId(newResourceID)
	}
	if d.HasChange("comment") {
		statement := fmt.Sprintf("ALTER SCHEMA %s SET COMMENT = '%s'", d.Id(), d.Get("comment"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("comment")
	}
	if d.HasChange("retention_time") {
		statement := fmt.Sprintf("ALTER SCHEMA %s SET DATA_RETENTION_TIME_IN_DAYS = %d", d.Id(), d.Get("retention_time"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("retention_time")
	}
	d.Partial(false)
	return nil
}

func resourceSnowflakeSchemaDelete(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	s := strings.Split(d.Id(), ".")
	database, name := s[0], s[1]
	exists, err := sqlObjExists(db, "schemas", name, database)
	if err != nil {
		return err
	}
	if exists == false {
		return fmt.Errorf("Schema %s does not exist", d.Id())
	}
	statement := fmt.Sprintf("DROP SCHEMA %s", d.Id())
	if _, err := db.Exec(statement); err != nil {
		return err
	}
	return nil
}
