package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

func resourceSnowflakeTableGrant() *schema.Resource {
	return &schema.Resource{
		Create: resourceSnowflakeTableGrantCreate,
		Read:   resourceSnowflakeTableGrantRead,
		Delete: resourceSnowflakeTableGrantDelete,
		Importer: &schema.ResourceImporter{
			State: func(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
				d.SetId(strings.ToUpper(d.Id()))
				return []*schema.ResourceData{d}, nil
			},
		},
		Schema: map[string]*schema.Schema{
			"table": {
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

func resourceSnowflakeTableGrantCreate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	table := strings.ToUpper(d.Get("table").(string))
	database := strings.ToUpper(d.Get("database").(string))
	schema := strings.ToUpper(d.Get("schema").(string))
	granteeRole := strings.ToUpper(d.Get("grantee_role").(string))
	granteeShare := strings.ToUpper(d.Get("grantee_share").(string))
	privileges := d.Get("privileges").([]string)

	id := ""

	if granteeRole != "" {
		id += fmt.Sprintf("%v.%v.%v.%v.", granteeRole, database, schema, table)
	} else {
		id += fmt.Sprintf("%v.%v.%v.%v.", granteeShare, database, schema, table)
	}

	statement := "GRANT "

	for _, p := range privileges {
		statement += p
		statement += ", "
		id += fmt.Sprintf("%v.", p)
	}
	statement = strings.Trim(statement, ", ")
	id = strings.Trim(id, ".")

	if table == "ALL" {
		statement += fmt.Sprintf("ON ALL TABLES IN %v.%v", database, schema)
	} else {
		statement += fmt.Sprintf("ON %v.%v.%v TO ", database, schema, table)
	}

	if granteeRole != "" {
		statement += fmt.Sprintf("ROLE %v", granteeRole)
	}

	if granteeShare != "" {
		statement += fmt.Sprintf("SHARE %v", granteeShare)
	}

	_, err := db.Exec(statement)
	if err != nil {
		return err
	}

	d.SetId(id)
	return nil
}

func resourceSnowflakeTableGrantRead(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	grantID := d.Id()
	s := strings.Split(grantID, ".")
	grantee, database, schema, table := s[0], s[1], s[2], s[3]
	tableGrantInfoResult, err := showTableGrant(db, grantee, database, schema, table)

	d.Set("privileges", tableGrantInfoResult.privileges)
	//This might not work but let's give it a try and see if i need to care whether the grantee is a role or a share
	d.Set("granteeRole", tableGrantInfoResult.grantee)
	d.Set("granteeShare", tableGrantInfoResult.grantee)
	d.Set("table", table) //maybe this works too?
	d.Set("schema", schema)
	d.Set("database", database)

	return err
}

func resourceSnowflakeTableGrantDelete(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	grantID := d.Id()
	s := strings.Split(grantID, ".")
	grantee, database, schema, table := s[0], s[1], s[2], s[3]
	statement := "REVOKE "

	for index, val := range s {
		if index >= 4 {
			statement += fmt.Sprintf("%v, ", val)
		}
	}

	statement = strings.Trim(statement, ", ")
	statement += fmt.Sprintf(" ON %v.%v.%v FROM %v", database, schema, table, grantee)

	_, err := db.Exec(statement)
	if err != nil {
		return err
	}
	return nil
}
