package snowflake

import (
	"crypto/sha256"
	"database/sql"
	b64 "encoding/base64"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

func getKeyFingerprint(key string) string {
	kb, _ := b64.StdEncoding.DecodeString(key)
	h := sha256.New()
	h.Write(kb)
	bs := h.Sum(nil)
	fp := b64.StdEncoding.EncodeToString(bs)
	fp = "SHA256:" + fp
	return fp
}

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
				Default:  false,
			},
			"default_role": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"default_warehouse": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"rsa_public_key": {
				Type:     schema.TypeString,
				Optional: true,
				StateFunc: func(v interface{}) string {
					return getKeyFingerprint(v.(string))
				},
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
	default_role := strings.ToUpper(d.Get("default_role").(string))
	default_warehouse := strings.ToUpper(d.Get("default_warehouse").(string))
	rsa_public_key := d.Get("rsa_public_key").(string)

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
	if default_role != "" {
		statement += fmt.Sprintf(" DEFAULT_ROLE = '%s'", default_role)
	}
	if default_warehouse != "" {
		statement += fmt.Sprintf(" DEFAULT_WAREHOUSE = '%s'", default_warehouse)
	}
	if rsa_public_key != "" {
		statement += fmt.Sprintf(" RSA_PUBLIC_KEY = '%s'", rsa_public_key)
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
	userInfo, err := descUser(db, name)
	d.Set("name", userInfo.name)
	d.Set("login_name", userInfo.login_name)
	d.Set("email", userInfo.email)
	d.Set("must_change_password", userInfo.must_change_password)
	d.Set("default_role", userInfo.default_role)
	d.Set("default_warehouse", userInfo.default_warehouse)
	d.Set("rsa_public_key", userInfo.rsa_public_key_fp)
	if err != nil {
		return err
	}
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
	if d.HasChange("default_role") {
		statement := fmt.Sprintf("ALTER USER %v SET DEFAULT_ROLE = '%v'", d.Id(), d.Get("default_role"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("default_role")
	}
	if d.HasChange("default_warehouse") {
		statement := fmt.Sprintf("ALTER USER %v SET DEFAULT_WAREHOUSE = '%v'", d.Id(), d.Get("default_warehouse"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("default_warehouse")
	}
	if d.HasChange("rsa_public_key") {
		statement := fmt.Sprintf("ALTER USER %v SET RSA_PUBLIC_KEY = '%v'", d.Id(), d.Get("rsa_public_key"))
		if _, err := db.Exec(statement); err != nil {
			return err
		}
		d.SetPartial("rsa_public_key")
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
