variable "snowflake_dsn" {}

provider "snowflake" {
  dsn = "${var.snowflake_dsn}"
}

resource "snowflake_database" "test" {
  name    = "test"
  comment = "this comment is a test that the comment parameter works"

  #   retention_time = 1
  #   transient      = true
}

resource "snowflake_schema" "test_schema" {
  database = "${snowflake_database.test.id}"
  schema   = "test_schema"
}

data "snowflake_schema" "test_public" {
  database = "${snowflake_database.test.id}"
  schema   = "public"
}

