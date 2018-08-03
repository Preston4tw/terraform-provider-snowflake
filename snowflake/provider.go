package snowflake

import (
	"database/sql"
	"log"

	// Snowflake SQL DB
	_ "github.com/snowflakedb/gosnowflake"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
)

// Provider returns a terraform.ResourceProvider.
func Provider() terraform.ResourceProvider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"dsn": &schema.Schema{
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("SNOWFLAKE_DSN", nil),
			},
		},
		DataSourcesMap: map[string]*schema.Resource{
			"snowflake_schema": dataSourceSnowflakeSchema(),
		},
		ResourcesMap: map[string]*schema.Resource{
			"snowflake_database":    resourceSnowflakeDatabase(),
			"snowflake_schema":      resourceSnowflakeSchema(),
			"snowflake_table":       resourceSnowflakeTable(),
			"snowflake_pipe":        resourceSnowflakePipe(),
			"snowflake_view":        resourceSnowflakeView(),
			"snowflake_user":        resourceSnowflakeUser(),
			"snowflake_stage":       resourceSnowflakeStage(),
			"snowflake_table_grant": resourceSnowflakeTableGrant(),
			"snowflake_view_grant":  resourceSnowflakeViewGrant(),
			"snowflake_role":        resourceSnowflakeRole(),
		},
		ConfigureFunc: providerConfigure,
	}
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	dsn := d.Get("dsn").(string)
	log.Printf("dsn: %q", dsn)
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	err = db.Ping()
	// _, err = db.Exec("select 1")
	if err != nil {
		return nil, err
	}
	return db, nil
}
