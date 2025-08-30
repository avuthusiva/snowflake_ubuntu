use role accountadmin;
use warehouse my_warehouse;
use schema my_db.my_schema;
show stages;
list @int_stage/json;
show file formats;
select $1 from @int_stage/json/simple_emp_single_entity.json.gz (file_format => 'json_format');
select * from table(infer_schema(location => '@int_stage/json/simple_emp_single_entity.json.gz', 
file_format => 'json_format'));
select $1 from @int_stage/json/emp_json_with_array.json.gz
(file_format => 'json_strip_format');

select * from table(infer_schema(location => '@int_stage/json/emp_json_with_array.json.gz', 
file_format => 'json_strip_format'));

select $1 from @int_stage/json/emp_json_with_comma.json.gz
(file_format => 'json_strip_format');
select * from table(infer_schema(location => '@int_stage/json/emp_json_with_comma.json.gz', 
file_format => 'json_strip_format'));

select t.key,t.value:address,t.value:created_at from (
select $1 as col from @int_stage/json/emp_json_with_dic.json.gz
(file_format => 'json_strip_format')),lateral flatten(input => parse_json(col)) as t;
select * from table(infer_schema(location => '@int_stage/json/emp_json_with_dic.json.gz', 
file_format => 'json_strip_format'));