use role accountadmin;
use warehouse my_warehouse;
use database my_db;
create schema control;
use schema my_db.control;
create table my_db.control.copy_ctrl
(
    id int autoincrement,
    stage_table_name string,
    schema_name string,
    database_name string,
    storage_integration string,
    storage_location string,
    files_type string,
    file_pattern string,
    filedelimiter string,
    filecompression string,
    on_error string,
    skip_header int,
    force boolean,
    trunc_col boolean,
    is_active boolean,
    created_on timestamp default current_timestamp()
);

select * from my_db.control.copy_ctrl;
desc table my_db.control.copy_ctrl;
