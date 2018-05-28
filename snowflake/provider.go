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
			"snowflake_database": resourceSnowflakeDatabase(),
			"snowflake_schema":   resourceSnowflakeSchema(),
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
	err = db.Ping()
	// _, err = db.Exec("select 1")
	if err != nil {
		return nil, err
	}
	return db, nil
}
