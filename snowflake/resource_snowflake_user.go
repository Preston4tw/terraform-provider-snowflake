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
			"email": {
				Type:     schema.TypeString,
				Optional: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
			},
			"must_change_password": {
				Type:     schema.TypeBool,
				Optional: true,
				Default: false,
			},
		},
	}
}

func resourceSnowflakeUserCreate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := strings.ToUpper(d.Get("name").(string))
	login_name := strings.ToUpper(d.Get("login_name").(string))
	email := strings.ToUpper(d.Get("email").(string))
	must_change_password := d.Get("must_change_password").(bool)

	statement := fmt.Sprintf("CREATE USER %v", name)
	if must_change_password == true {
		statement += fmt.Sprintf(" MUST_CHANGE_PASSWORD = TRUE")
	}
	if login_name != "" {
		statement += fmt.Sprintf(" LOGIN_NAME = '%s'", login_name)
	}
	if email != "" {
		statement += fmt.Sprintf(" EMAIL = '%s'", email)
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
	d.Set("login_name", userInfo.login_name)
	d.Set("email", userInfo.email)
	d.Set("must_change_password", userInfo.must_change_password)

	return nil
}

func resourceSnowflakeUserUpdate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := d.Id()
	exists, err := sqlObjExists(db, "users", name, "account")

	if err != nil {
		return err
	}
	if exists == false {
		return fmt.Errorf("User %s does not exist", d.Id())
	}
	// Rather than issue a single alter user statement for all possible
	// changes issue an alter for each possible thing that has changed. Enable
	// partial mode.
	d.Partial(true)
	if d.HasChange("name") {
		// check that the rename target does not exist
		exists, err := sqlObjExists(db, "users", d.Get("name").(string), "account")
		if err != nil {
			return err
		}
		if exists == true {
			return fmt.Errorf("Cannot rename %v to %v, %v already exists", d.Id(), d.Get("name"), d.Get("name"))
		}
		statement := fmt.Sprintf("ALTER USER %v RENAME TO %v", d.Id(), d.Get("name"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("name")
		d.SetId(d.Get("name").(string))

	}
	if d.HasChange("email") {
		statement := fmt.Sprintf("ALTER USER %v SET EMAIL = '%v'", d.Id(), d.Get("email"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("email")
	}
	if d.HasChange("login_name") {
		statement := fmt.Sprintf("ALTER USER %v SET LOGIN_NAME = '%v'", d.Id(), d.Get("login_name"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("login_name")
	}
	if d.HasChange("must_change_password") {
		statement := fmt.Sprintf("ALTER USER %v SET MUST_CHANGE_PASSWORD = %v", d.Id(), d.Get("must_change_password"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("must_change_password")
	}
	d.Partial(false)
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
