package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

func resourceSnowflakeUser() *schema.Resource {
	return &schema.Resource{
		Create: resourceSnowflakeUserCreate,
		Read:   resourceSnowflakeUserRead,
		Update: resourceSnowflakeUserUpdate,
		Delete: resourceSnowflakeUserDelete,
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
			"login_name": {
				Type:     schema.TypeString,
				Optional: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
			},
		},
	}
}

func resourceSnowflakeUserCreate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := strings.ToUpper(d.Get("name").(string))
	login_name := strings.ToUpper(d.Get("login_name").(string))

	statement := fmt.Sprintf("CREATE USER %v", name)

	//append login_name if not null
	if login_name != "" {
		statement += fmt.Sprintf(" LOGIN_NAME = '%s'", login_name)
	}

	_, err := db.Exec(statement)
	if err != nil {
		return err
	}
	d.SetId(name)
	return nil
}

func resourceSnowflakeUserRead(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := d.Id()
	userInfo, err := showUser(db, name)
	if err != nil {
		return err
	}
	d.Set("name", userInfo.name)
	return nil
}

func resourceSnowflakeUserUpdate(d *schema.ResourceData, meta interface{}) error {
	// Update comment
	return nil
}

func resourceSnowflakeUserDelete(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := d.Id()
	exists, err := sqlObjExists(db, "users", name, "account")
	if err != nil {
		return err
	}
	if exists == false {
		return fmt.Errorf("USER %s does not exist", d.Id())
	}
	statement := fmt.Sprintf("DROP USER %v", d.Get("name"))
	if _, err := db.Exec(statement); err != nil {
		return err
	}
	return nil
}
