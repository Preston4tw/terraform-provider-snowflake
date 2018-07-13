# terraform-provider-snowflake

terraform-provider-snowflake is a [Terraform](https://www.terraform.io/) provider for the [Snowflake](https://www.snowflake.net/) cloud data warehouse.

## Supported types

### Resources

- snowflake_database
- snowflake_schema
- snowflake_pipe
- snowflake_table
- snowflake_view
- snowflake_user
- snowflake_stage

### Data Sources

- snowflake_schema

## Configuring the provider

Right now the provider is configured by providing the full DSN which is fed through to the gosnowflake connector. Here are some examples of the format of the DSN:

```text
user[:password]@account/database/schema[?param1=value1&paramN=valueN]
user[:password]@account/database[?param1=value1&paramN=valueN]
user[:password]@host:port/database/schema?account=user_account[?param1=value1&paramN=valueN]
```

If a variable is set up for the DSN it can be configured as an environment variable or in `terraform.tfvars`.

## Of note

The provider needs an active warehouse to run queries against information_schema for refreshing resources. You have to pre-create an auto-resuming warehouse and specify that warehouse in the DSN as a parameter (ex. `?warehouse=myWarehouse`).
