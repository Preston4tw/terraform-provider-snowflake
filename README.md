# terraform-provider-snowflake

terraform-provider-snowflake is a [Terraform](https://www.terraform.io/) provider for the [Snowflake](https://www.snowflake.net/) cloud data warehouse.

This project is abanonded. I recommend you look to [this](https://github.com/chanzuckerberg/terraform-provider-snowflake) project instead which may be actively maintained.

## Why

Getting data into Snowflake without terraform-provider-snowflake involves a lot of tedious back and forth between AWS and Snowflake. Snowflake expects you have a s3 bucket somewhere with your data, and then you grant Snowflake access to that S3 bucket. Doing that involves creating either a user and access keys or an IAM role with a trust policy that allows Snowflake to assume the role with access to the bucket. As Terraform already manages AWS resources, by extending Terraform to support Snowflake, Terraform can be leveraged to make onboarding data easier.

## Supported types

### Resources

- snowflake_database
- snowflake_schema
- snowflake_pipe
- snowflake_table
- snowflake_view
- snowflake_user
- snowflake_stage
- snowflake_role
- snowflake_table_grant
- snowflake_view_grant

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
