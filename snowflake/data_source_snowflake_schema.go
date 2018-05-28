package snowflake

import (
	"database/sql"
	"fmt"

	"github.com/hashicorp/terraform/helper/schema"
)

func dataSourceSnowflakeSchema() *schema.Resource {
	return &schema.Resource{
		Read: dataSourceSnowflakeSchemaRead,

		Schema: map[string]*schema.Schema{
			"schema": {
				Type:     schema.TypeString,
				Required: true,
			},
			"database": {
				Type:     schema.TypeString,
				Required: true,
			},
			"owner": {
				Type:     schema.TypeString,
				Computed: true,
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
	statement := fmt.Sprintf("SHOW SCHEMAS LIKE '%v' in %v", d.Get("schema"), d.Get("database"))

	rows, err := db.Query(statement)
	if err != nil {
		return err
	}
	defer rows.Close()

	index := 0

	for rows.Next() {
		index = index + 1
		if index > 1 {
			return fmt.Errorf("More than 1 row returned for query '%v'", statement)
		}
		var r showSchemaRow
		if err := rows.Scan(
			&r.createdOn,
			&r.name,
			&r.isDefault,
			&r.isCurrent,
			&r.databaseName,
			&r.owner,
			&r.comment,
			&r.options,
			&r.retentionTime,
		); err != nil {
			return err
		}
		d.SetId(fmt.Sprintf("%s.%s", r.databaseName, r.name))
		d.Set("schema", r.name)
		d.Set("database", r.databaseName)
		d.Set("owner", r.owner)
		d.Set("comment", r.comment)
		if r.options == "TRANSIENT" {
			d.Set("transient", true)
		} else {
			d.Set("transient", false)
		}
		d.Set("retention_time", r.retentionTime)
	}
	if index == 0 {
		return fmt.Errorf("No schemas found for query '%v'", statement)
	}
	return nil
}
