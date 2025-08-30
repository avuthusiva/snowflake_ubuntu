from snowflake.snowpark import Session

conn_params = {
    "account": "VYLRAHU-UG05883",
    "user": "asvr410",
    "password": "RA-one41132025",
    "warehouse": "my_warehouse",
    "database": "my_db",
    "schema": "my_schema",
    "role": "ACCOUNTADMIN"
}

session = Session.builder.configs(conn_params).create()
session.sql("select current_date() ").show()