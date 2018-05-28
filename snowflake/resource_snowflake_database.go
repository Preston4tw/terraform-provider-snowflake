package snowflake

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

// TODO: Implement Clone parameter of create

func resourceSnowflakeDatabase() *schema.Resource {
	return &schema.Resource{
		Create: resourceSnowflakeDatabaseCreate,
		Read:   resourceSnowflakeDatabaseRead,
		Update: resourceSnowflakeDatabaseUpdate,
		Delete: resourceSnowflakeDatabaseDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
				// ValidateFunc: validateDatabaseName,
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
				ForceNew: true,
			},
			"retention_time": {
				Type:     schema.TypeInt,
				Default:  0,
				Optional: true,
			},
		},
		// Importer:
	}
}

func resourceSnowflakeDatabaseCreate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	statement := fmt.Sprintf("CREATE DATABASE %v DATA_RETENTION_TIME_IN_DAYS = %d", d.Get("name"), d.Get("retention_time"))
	if d.Get("transient").(bool) == true {
		statement = fmt.Sprintf("CREATE TRANSIENT DATABASE %v DATA_RETENTION_TIME_IN_DAYS = %d", d.Get("name"), d.Get("retention_time"))
		d.Set("transient", true)
	} else {
		d.Set("transient", false)
	}
	if d.Get("comment") != "" {
		statement += fmt.Sprintf(" COMMENT = '%v'", d.Get("comment"))
	}
	_, err := db.Exec(statement)
	if err != nil {
		return err
	}
	d.SetId(strings.ToUpper(d.Get("name").(string)))
	return nil
}

func resourceSnowflakeDatabaseRead(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	databaseInfo, err := showDatabase(db, d.Id())
	if err != nil {
		return err
	}
	d.Set("name", databaseInfo.name)
	d.Set("owner", databaseInfo.owner)
	d.Set("comment", databaseInfo.comment)
	if databaseInfo.options == "TRANSIENT" {
		d.Set("transient", true)
	} else {
		d.Set("transient", false)
	}
	if databaseInfo.retentionTime != "" {
		retentionTime, err := strconv.Atoi(databaseInfo.retentionTime)
		if err != nil {
			return err
		}
		d.Set("retention_time", retentionTime)
	}
	return nil
}

func resourceSnowflakeDatabaseUpdate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	exists, err := databaseExists(db, d.Id())
	if err != nil {
		return err
	}
	if exists == false {
		return fmt.Errorf("Database %s does not exist", d.Id())
	}
	// statement := fmt.Sprintf("")
	// Rather than issue a single alter database statement for all possible
	// changes issue an alter for each possible thing that has changed. Enable
	// partial mode.
	d.Partial(true)
	if d.HasChange("name") {
		// check that the rename target does not exist
		destExists, err := databaseExists(db, d.Get("name").(string))
		if err != nil {
			return err
		}
		if destExists == true {
			return fmt.Errorf("Cannot rename %v to %v, %v already exists", d.Id(), d.Get("name"), d.Get("name"))
		}
		statement := fmt.Sprintf("ALTER DATABASE %v RENAME TO %v", d.Id(), d.Get("name"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("name")
		d.SetId(d.Get("name").(string))

	}
	if d.HasChange("comment") {
		statement := fmt.Sprintf("ALTER DATABASE %v SET COMMENT = '%v'", d.Id(), d.Get("comment"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("comment")
	}
	if d.HasChange("retention_time") {
		statement := fmt.Sprintf("ALTER DATABASE %v SET DATA_RETENTION_TIME_IN_DAYS = %d", d.Id(), d.Get("retention_time"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("retention_time")
	}
	d.Partial(false)
	return nil
}

func resourceSnowflakeDatabaseDelete(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	// https://docs.snowflake.net/manuals/sql-reference/identifiers-syntax.html
	// As long as identifiers are not double quoted they are not case sensitive
	// and multiple resources for a name are impossible. This is a check
	// against the case where databases were created with double quotes using
	// the same name with different casing
	exists, err := databaseExists(db, d.Id())
	if err != nil {
		return err
	}
	if exists == false {
		return fmt.Errorf("Database %s does not exist", d.Id())
	}
	statement := fmt.Sprintf("DROP DATABASE %v", d.Get("name"))
	if _, err := db.Exec(statement); err != nil {
		return err
	}
	return nil
}
