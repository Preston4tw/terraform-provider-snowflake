variable "snowflake_dsn" {}

provider "snowflake" {
  dsn = "${var.snowflake_dsn}"
}

resource "snowflake_database" "test1" {
  name    = "test1"
  comment = "this comment is a test that the comment parameter works"

  #   retention_time = 1
  #   transient      = true
}

resource "snowflake_database" "test2" {
  name = "test2"
}

resource "snowflake_schema" "test1_dev" {
  name     = "dev"
  database = "${snowflake_database.test1.id}"
}

resource "snowflake_schema" "test2_prod" {
  name     = "prod"
  database = "${snowflake_database.test2.id}"
}

resource "snowflake_table" "prod" {
  name     = "prod"
  database = "${snowflake_schema.test2_prod.database}"
  schema   = "${snowflake_schema.test2_prod.name}"

  columns = [
    {
      name = "EVENTTIME"
      type = "TIMESTAMP_TZ(9)"
    },
    {
      name = "V"
      type = "VARIANT"
    },
  ]
}
