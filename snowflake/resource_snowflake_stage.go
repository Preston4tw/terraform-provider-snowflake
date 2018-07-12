package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

func resourceSnowflakeStage() *schema.Resource {
	return &schema.Resource{
		Create: resourceSnowflakeStageCreate,
		Read:   resourceSnowflakeStageRead,
		//Update: resourceSnowflakeStageUpdate,
		Delete: resourceSnowflakeStageDelete,
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
				Optional: true,
				StateFunc: func(v interface{}) string {
					return strings.ToUpper(v.(string))
				},
				Default:  "PUBLIC",
				ForceNew: true,
			},
			"url": {
				Type:     schema.TypeString,
				Optional: true,
				StateFunc: func(v interface{}) string {
					return strings.ToLower(v.(string))
				},
				ForceNew: true,
			},
			"credentials": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"aws_role"},
			},
			"aws_role": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"credentials"},
			},
			"aws_external_id": {
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
			},
			"snowflake_iam_user": {
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
			},
		},
	}
}

func resourceSnowflakeStageCreate(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := strings.ToUpper(d.Get("name").(string))
	database := strings.ToUpper(d.Get("database").(string))
	schema := strings.ToUpper(d.Get("schema").(string))
	stageId := fmt.Sprintf("%s.%s.%s", database, schema, name)
	url := d.Get("url")
	credentials := d.Get("credentials")
	aws_role := d.Get("aws_role")

	statement := fmt.Sprintf("CREATE STAGE %v.%v.%v", database, schema, name)
	if url != "" {
		statement += fmt.Sprintf(" URL = '%v'", url)
	}
	if credentials != "" {
		statement += fmt.Sprintf(" CREDENTIALS = (%v)", credentials)
	}
	if aws_role != "" {
		statement += fmt.Sprintf(" CREDENTIALS = (%v)", aws_role)
	}

	_, err := db.Exec(statement)
	if err != nil {
		return err
	}
	d.SetId(stageId)
	return nil
}

func resourceSnowflakeStageRead(d *schema.ResourceData, meta interface{}) error {

	db := meta.(*sql.DB)
	stageId := d.Id()
	s := strings.Split(stageId, ".")
	database, schema, name := s[0], s[1], s[2]
	stageInfo, err := descStage(db, database, schema, name)
	if err != nil {
		return err
	}
	d.Set("name", name)
	d.Set("schema", schema)
	d.Set("database", database)
	d.Set("url", stageInfo.url)
	if stageInfo.aws_role != "" {
		d.Set("aws_role", stageInfo.aws_role)
	}
	if stageInfo.aws_external_id != "" {
		d.Set("aws_external_id", stageInfo.aws_external_id)
	}
	if stageInfo.snowflake_iam_user != "" {
		d.Set("snowflake_iam_user", stageInfo.snowflake_iam_user)
	}

	return nil
}

/*
func resourceSnowflakeStageUpdate(d *schema.ResourceData, meta interface{}) error {
	return nil
}*/

func resourceSnowflakeStageDelete(d *schema.ResourceData, meta interface{}) error {

	db := meta.(*sql.DB)
	stageId := d.Id()
	statement := fmt.Sprintf("DROP STAGE %v", stageId)
	_, err := db.Exec(statement)
	if err != nil {
		return err
	}

	return nil
}
