package snowflake

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

func dataSourceSnowflakeSchema() *schema.Resource {
	return &schema.Resource{
		Read: dataSourceSnowflakeSchemaRead,

		Schema: map[string]*schema.Schema{
			"schema": {
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
				Computed: true,
			},
			"transient": {
				Type:     schema.TypeBool,
				Computed: true,
			},
			"retention_time": {
				Type:     schema.TypeInt,
				Computed: true,
			},
		},
	}
}

func dataSourceSnowflakeSchemaRead(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	database := d.Get("database").(string)
	schema := d.Get("schema").(string)
	schemaInfo, err := showSchema(db, database, schema)
	if err != nil {
		return err
	}
	d.SetId(strings.ToUpper(fmt.Sprintf("%s.%s", database, schema)))
	d.Set("schema", schemaInfo.name)
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
