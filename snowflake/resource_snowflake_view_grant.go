package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

func resourceSnowflakeViewGrant() *schema.Resource {
	return &schema.Resource{
		Create: resourceSnowflakeViewGrantCreate,
		Read:   resourceSnowflakeViewGrantRead,
		Delete: resourceSnowflakeViewGrantDelete,
		Importer: &schema.ResourceImporter{
			State: func(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
				d.SetId(strings.ToUpper(d.Id()))
				return []*schema.ResourceData{d}, nil
			},
		},
		Schema: map[string]*schema.Schema{
			"view": {
				Type:     schema.TypeString,
				Required: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
				ForceNew: true,
			},
			"database": {
				Type:     schema.TypeString,
				Required: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
				ForceNew: true,
			},
			"schema": {
				Type:     schema.TypeString,
				Required: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
				ForceNew: true,
			},
			"privileges": {
				Type: schema.TypeList,
				Elem: &schema.Schema{
					Type: schema.TypeString,
					StateFunc: func(v interface{}) string {
						return strings.ToUpper(v.(string))
					},
				},
				Required: true,
				ForceNew: true,
			},
			"grantee_role": {
				Type:     schema.TypeString,
				Optional: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
				ForceNew:      true,
				ConflictsWith: []string{"grantee_share"},
			},
			"grantee_share": {
				Type:     schema.TypeString,
				Optional: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
				ForceNew:      true,
				ConflictsWith: []string{"grantee_role"},
			},
		},
	}
}

func resourceSnowflakeViewGrantCreate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	view := strings.ToUpper(d.Get("view").(string))
	database := strings.ToUpper(d.Get("database").(string))
	schema := strings.ToUpper(d.Get("schema").(string))
	granteeRole := strings.ToUpper(d.Get("grantee_role").(string))
	granteeShare := strings.ToUpper(d.Get("grantee_share").(string))

	id := ""

	if granteeRole != "" {
		id += fmt.Sprintf("%v.%v.%v.%v.", granteeRole, database, schema, view)
	} else {
		id += fmt.Sprintf("%v.%v.%v.%v.", granteeShare, database, schema, view)
	}

	statement := "GRANT "

	for _, p := range d.Get("privileges").([]interface{}) {
		statement += p.(string)
		statement += ", "
		id += fmt.Sprintf("%v.", p)
	}
	statement = strings.Trim(statement, ", ")
	id = strings.Trim(id, ".")

	if view == "ALL" {
		statement += fmt.Sprintf(" ON ALL VIEWS IN %v.%v", database, schema)
	} else {
		statement += fmt.Sprintf(" ON %v.%v.%v TO ", database, schema, view)
	}

	if granteeRole != "" {
		statement += fmt.Sprintf("ROLE %v", granteeRole)
	}

	if granteeShare != "" {
		statement += fmt.Sprintf("SHARE %v", granteeShare)
	}

	statement = strings.ToUpper(statement)

	_, err := db.Exec(statement)
	if err != nil {
		return err
	}

	d.SetId(id)
	return nil
}

func resourceSnowflakeViewGrantRead(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	grantID := d.Id()
	s := strings.Split(grantID, ".")
	grantee, database, schema, view := s[0], s[1], s[2], s[3]
	ViewGrantInfoResult, err := showViewGrant(db, grantee, database, schema, view)

	d.Set("privileges", ViewGrantInfoResult.privileges)
	d.Set("granteeRole", ViewGrantInfoResult.grantee)
	d.Set("granteeShare", ViewGrantInfoResult.grantee)
	d.Set("view", view)
	d.Set("schema", schema)
	d.Set("database", database)

	return err
}

func resourceSnowflakeViewGrantDelete(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	grantID := d.Id()
	s := strings.Split(grantID, ".")
	grantee, database, schema, view := s[0], s[1], s[2], s[3]
	statement := "REVOKE "

	for index, val := range s {
		if index >= 4 {
			statement += fmt.Sprintf("%v, ", val)
		}
	}

	statement = strings.Trim(statement, ", ")
	statement += fmt.Sprintf(" ON %v.%v.%v FROM %v", database, schema, view, grantee)

	statement = strings.ToUpper(statement)

	_, err := db.Exec(statement)
	if err != nil {
		return err
	}
	return nil
}
