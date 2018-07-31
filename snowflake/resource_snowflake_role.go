package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

func resourceSnowflakeRole() *schema.Resource {
	return &schema.Resource{
		Create: resourceSnowflakeRoleCreate,
		Read:   resourceSnowflakeRoleRead,
		Update: resourceSnowflakeRoleUpdate,
		Delete: resourceSnowflakeRoleDelete,
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
			"comment": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},
	}
}

func resourceSnowflakeRoleCreate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := strings.ToUpper(d.Get("name").(string))
	comment := d.Get("comment").(string)

	statement := fmt.Sprintf("CREATE ROLE %v", name)

	if comment != "" {
		statement += fmt.Sprintf(" COMMENT = '%v'", comment)
	}

	_, err := db.Exec(statement)
	if err != nil {
		return err
	}

	d.SetId(name)

	return nil
}

func resourceSnowflakeRoleRead(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := d.Id()
	showRoleRow, err := showRole(db, name)
	if err != nil {
		return err
	}
	d.Set("name", showRoleRow.name)
	d.Set("comment", showRoleRow.comment)

	return nil
}

func resourceSnowflakeRoleUpdate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := d.Id()
	exists, err := sqlObjExists(db, "roles", name, "account")

	if err != nil {
		return err
	}

	if exists == false {
		return fmt.Errorf("Role %s does not exist", d.Id())
	}

	d.Partial(true)
	if d.HasChange("name") {
		// check that the rename target does not exist
		exists, err := sqlObjExists(db, "roles", d.Get("name").(string), "account")
		if err != nil {
			return err
		}
		if exists == true {
			return fmt.Errorf("Cannot rename %v to %v, %v already exists", d.Id(), d.Get("name"), d.Get("name"))
		}
		statement := fmt.Sprintf("ALTER ROLE %v RENAME TO %v", d.Id(), d.Get("name"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("name")
		d.SetId(d.Get("name").(string))
	}

	if d.HasChange("comment") {
		var statement string
		if d.Get("comment") == "" {
			statement = fmt.Sprintf("ALTER ROLE %v UNSET COMMENT", d.Id())
		} else {
			statement = fmt.Sprintf("ALTER ROLE %v SET COMMENT = '%v'", d.Id(), d.Get("comment"))
		}
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("comment")
	}
	d.Partial(false)
	return nil
}

func resourceSnowflakeRoleDelete(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := d.Id()
	exists, err := sqlObjExists(db, "roles", name, "account")
	if err != nil {
		return err
	}
	if exists == false {
		return fmt.Errorf("ROLE %s does not exist", d.Id())
	}
	statement := fmt.Sprintf("DROP ROLE %v", d.Get("name"))
	if _, err := db.Exec(statement); err != nil {
		return err
	}
	return nil
}
