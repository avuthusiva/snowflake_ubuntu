use role accountadmin;
use warehouse my_warehouse;
use database my_db;
create schema github_schema;
use schema my_db.github_schema;
create or replace api integration github_int
enabled = true
api_provider = git_https_api
api_allowed_prefixes = ('https://github.com/avuthusiva/retail_db.git');
create or replace git repository github_repo
api_integration = github_int
origin = 'https://github.com/avuthusiva/retail_db.git';
show integrations;
show api integrations;
show git repositories;
desc git repository github_repo;
show git branches in github_repo;
ls @github_repo/branches/master;
select $1,$2,$3,$4,metadata$filename,metadata$file_row_number from 
@github_repo/branches/master/orders/part-00000 
(file_format => my_db.my_schema.csv_format_skip_header);
alter git repository github_repo fetch;
execute immediate from @github_repo/branches/master/create_db_tables_pg.sql;
show tables;
execute immediate from @github_repo/branches/master/load_db_tables_pg.sql;
select * from  orders;
select * from order_items;
alter git repository github_repo fetch;
ls @github_repo/branches/master;
execute immediate from @github_repo/branches/master/test_data_insert.sql;
select * from test_data;