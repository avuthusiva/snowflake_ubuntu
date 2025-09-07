use role accountadmin;
use warehouse my_warehouse;
use schema my_db.practice;
show tables;
select * from region;
select * from (values (1,'JAN'),(2,'FEB'),(3,'MAR'),(4,'APR'),(5,'MAY'),(6,'JUN'),(7,'JUL'),(8,'AUG'),
(9,'SEP'),(10,'OCT'),(11,'NOV'),(12,'DEC')) as months(month, month_name);
show file formats;
create or replace stage data_stage
url = 's3://sfquickstarts/tasty-bytes-builder-education/'
directory = (enable = True);
ls @data_stage;
select '@data_stage/' || relative_path,size from directory(@data_stage) 
where relative_path like '%menu%';
select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14 from @data_stage/raw_pos/menu/menu.csv.gz
(file_format=> CSV_FORMAT_NO_HEADER);
select * from table(infer_schema(location => '@data_stage/raw_pos/menu/menu.csv.gz',
file_format => 'CSV_FORMAT_PARSE_HEADER'));