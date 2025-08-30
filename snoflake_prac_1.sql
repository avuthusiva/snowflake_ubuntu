use role accountadmin;

use warehouse my_warehouse;

use schema my_db.my_schema;
select current_database(),current_schema();

show integrations;

desc integration aws_int;

desc stage aws_s3_stage;

show stages in my_db.my_schema;

list @my_db.my_schema.aws_s3_stage;

select * from directory('@my_db.my_schema.aws_s3_stage');

select $1,$2,$3,$4,$5,$6 from '@my_db.my_schema.aws_s3_stage/csv/4 - customer.csv' 
(file_format => my_db.my_schema.CSV_FORMAT_SKIP_HEADER);

show file formats in my_db.my_schema;

create table customer using template (
select array_agg(object_construct(*)) within group(order by order_id)
from table(infer_schema(location => '@my_db.my_schema.aws_s3_stage/csv/4 - customer.csv',
file_format => 'my_db.my_schema.CSV_FORMAT_HEADER')));

desc file format my_db.my_schema.CSV_FORMAT_HEADER;

alter file format my_db.my_schema.CSV_FORMAT_HEADER set FIELD_OPTIONALLY_ENCLOSED_BY = '"';

select $1,$2,$3,$4,$5,$6,$7,$8,metadata$filename,metadata$file_row_number,
metadata$file_last_modified::date
from '@my_db.my_schema.aws_s3_stage/csv/4 - customer.csv'
(file_format => my_db.my_schema.CSV_FORMAT_SKIP_HEADER);

select * from customer;

copy into customer from '@my_db.my_schema.aws_s3_stage/csv/4 - customer.csv' 
file_format = (format_name = my_db.my_schema.CSV_FORMAT_SKIP_HEADER)
on_error = 'CONTINUE';

select * from customer;

truncate table customer;

select * from table(information_schema.copy_history(table_name => 'customer',
start_time => dateadd(hours,-1,current_timestamp())));

desc file format my_db.my_schema.CSV_FORMAT_SKIP_HEADER;

alter file format my_db.my_schema.CSV_FORMAT_SKIP_HEADER set trim_space = True;

create or replace file format json_format 
type = json;
create file format json_strip_format 
type = json
strip_outer_array= true;

select * from directory('@my_db.my_schema.aws_s3_stage');
alter stage aws_s3_stage refresh;
select typeof($1) from '@aws_s3_stage/json/employee_array.json' (file_format => json_format);
select $1:employee_id,$1:address from '@aws_s3_stage/json/employee_object.json' (file_format => json_format);
select $1:address,($1:skills) from '@aws_s3_stage/json/employee_E105.json' (file_format => json_format);
select $1 
from '@aws_s3_stage/json/employees_multi_json_array.json' (file_format => json_format);
select col:employee_id,col:employee_name,col:position,phone.value,col:address,skills.value:proficiency_level
,skills.value:skill_name
from (select $1 col from '@aws_s3_stage/json/employee_E105.json' (file_format => json_format)) tab,
lateral flatten(col:phone_numbers) phone,lateral flatten(col:skills) skills;

select c:employee_id,c:employee_name,c:position,
ph.value,
c:address:city,c:address:state,c:address:street,c:address:zip_code,
sk.value:proficiency_level,sk.value:skill_name from (
select $1 c
from '@aws_s3_stage/json/employees_multi_json_array.json' (file_format => json_strip_format)) tab,
lateral flatten(c:phone_numbers) ph,lateral flatten(c:skills) sk
where c:employee_id = '103';

select * from directory('@aws_s3_stage');

alter stage aws_s3_stage refresh;

select $1::timestamp,$2,$3,$4,$5,$6,$7,$8,$9,$10::date,$11,$12,$13,metadata$filename
metadata$file_row_number
from '@aws_s3_stage/snowpipe/csv/userdata1.csv' 
(file_format => csv_format_skip_header);

show file formats;

create table userdata using template (
select array_agg(object_construct(*)) from table(infer_schema(location =>'@aws_s3_stage/snowpipe/csv/',
file_format => 'CSV_FORMAT_HEADER')));

select * from userdata;

alter table userdata add (filename text,filerownumber number);

copy into userdata from (select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,metadata$filename,
metadata$file_row_number
from '@aws_s3_stage/snowpipe/csv/userdata1.csv' 
(file_format => csv_format_skip_header)) pattern = '.*userdata.*[.]csv' 
file_format = (format_name = csv_format_skip_header)
on_error = 'CONTINUE';

select * from information_schema.columns where table_name = 'USERDATA';

create pipe userdata_pipe
auto_ingest = true
as
copy into userdata from (select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,metadata$filename,
metadata$file_row_number
from '@aws_s3_stage/snowpipe/csv/' 
(file_format => csv_format_skip_header)) pattern = '.*userdata.*[.]csv' 
file_format = (format_name = csv_format_skip_header)
on_error = 'CONTINUE';

desc pipe userdata_pipe;

select system$pipe_status('userdata_pipe');

select * from table(information_schema.copy_history(table_name=>'USERDATA',
start_time => DATEADD(hours,-1,current_timestamp())));

select * from table(information_schema.pipe_usage_history());

select * from table(information_schema.validate_pipe_load(pipe_name => 'userdata_pipe',
start_time => dateadd(hours,-1,current_timestamp())));

select * from userdata where "comments" is not null;

alter pipe userdata_pipe refresh;

alter pipe userdata_pipe set pipe_execution_paused = true;

drop pipe userdata_pipe;

select * from information_schema.tables where table_schema = 'MY_SCHEMA';

select * from directory('@aws_s3_stage');

show file formats;
create table dept using template (
select array_agg(object_construct(*)) within group( order by order_id) from table(infer_schema(location => '@aws_s3_stage/csv/DEPT.csv'
,file_format => 'CSV_FORMAT_HEADER')));

select * from dept;

copy into dept from '@aws_s3_stage/csv/DEPT.csv' 
file_format = (format_name = csv_format_skip_header)
on_error = 'CONTINUE';
create table emp using template (
select array_agg(object_construct(*)) within group(order by order_id) 
from table(infer_schema(location => '@aws_s3_stage/csv/EMP.csv',
file_format => 'csv_format_header')));

select * from emp;

copy into emp from '@aws_s3_stage/csv/EMP.csv'
file_format = (format_name = csv_format_skip_header)
on_error = 'CONTINUE';

select * from dept d,lateral(
select deptno,sum(sal) sum_sal,avg(sal) avg_sal,min(sal) min_sal,max(sal) max_sal from emp e
where e.deptno = d.deptno
group by deptno);

create stream dept_stream on table dept;

select * from dept_stream;

alter table dept add (create_date timestamp);

select * from dept;
update dept set create_date = current_timestamp();
alter table dept drop column create_date;
create schema dev;
create schema prod;
use schema my_db.my_schema;
create table my_db.dev.dept using template(
select array_agg(object_construct(*)) within group(order by order_id) from table(infer_schema(location => '@aws_s3_stage/csv/DEPT.csv'
,file_format =>'csv_format_header')));

create table my_db.prod.dept using template(
select array_agg(object_construct(*)) within group(order by order_id) from table(infer_schema(location => '@aws_s3_stage/csv/DEPT.csv'
,file_format =>'csv_format_header')));

select * from my_db.dev.dept;

select * from my_db.prod.dept;
create stream my_db.dev.dept_stream on table my_db.dev.dept;
create or replace task dept_task
warehouse = my_warehouse
schedule = '1 MINUTE'
when system$stream_has_data('my_db.dev.dept_stream')
as
merge into my_db.prod.dept d
using my_db.dev.dept_stream s
on(s.deptno = d.deptno)
when matched and metadata$action = 'INSERT' and metadata$isupdate = true
then
    update set d.dname = s.dname,d.loc = s.loc
when matched and metadata$action = 'DELETE' and metadata$isupdate = false
then
    delete
when not matched and metadata$action = 'INSERT' and metadata$isupdate = false
then 
    insert values (s.deptno,s.dname,s.loc);

show tasks;

alter task dept_task resume;

insert into my_db.dev.dept values (20,'HR','BAN');

select * from my_db.dev.dept;

update my_db.dev.dept set loc = 'CHN'
where deptno = 10;

delete from my_db.dev.dept where deptno = 10;

select * from my_db.dev.dept_stream;

select * from my_db.prod.dept;

drop task dept_task;

select current_database(),current_schema();

CREATE TABLE customer (
    customer_id INT NOT NULL,
    name VARCHAR(50) NOT NULL,
    visited_on DATE NOT NULL,
    amount DECIMAL(10,2) NOT NULL
);
INSERT INTO customer (customer_id, name, visited_on, amount) VALUES
(1, 'Jhon',    '2019-01-01', 100),
(2, 'Daniel',  '2019-01-02', 110),
(3, 'Jade',    '2019-01-03', 120),
(4, 'Khaled',  '2019-01-04', 130),
(5, 'Winston', '2019-01-05', 110),
(6, 'Elvis',   '2019-01-06', 140),
(7, 'Anna',    '2019-01-07', 150),
(8, 'Maria',   '2019-01-08', 80),
(9, 'Jaze',    '2019-01-09', 110),
(1, 'Jhon',    '2019-01-10', 130),
(3, 'Jade',    '2019-01-10', 150);

select * from customer;

select visited_on,amt,avg from (
select visited_on,sum(amount) over(order by visited_on)- sum(lag_amount) over(order by visited_on) amt,
round((sum(amount) over(order by visited_on)- sum(lag_amount) over(order by visited_on))/7,2) avg,
lag_amount from
(select visited_on,amount,lag(amount,7,0) over(order by visited_on) lag_amount from (
select visited_on,sum(amount) amount from customer
group by visited_on)))
where lag_amount <> 0;

CREATE TABLE emp_details (
    emp_name VARCHAR(10),
    city VARCHAR(15)
);

-- Insert sample data
INSERT INTO emp_details (emp_name, city) VALUES
('Sam', 'New York'),
('David', 'New York'),
('Peter', 'New York'),
('Chris', 'New York'),
('John', 'New York'),
('Steve', 'San Francisco'),
('Rachel', 'San Francisco'),
('Robert', 'Los Angeles');

select * from emp_details;

with cte as 
(select emp_name,city,row_number() over(partition by city order by emp_name) n from emp_details)
select city,listagg(emp_name,','),'Team' || row_number() over(order by city)  from (
select city,emp_name,floor((n-1)/3) + 1 gp from cte)
group by city,gp;

CREATE TABLE airports (
    port_code VARCHAR(10) PRIMARY KEY,
    city_name VARCHAR(100)
);

CREATE TABLE flights (
    flight_id varchar (10),
    start_port VARCHAR(10),
    end_port VARCHAR(10),
    start_time datetime,
    end_time datetime
);

--delete from airports;
INSERT INTO airports (port_code, city_name) VALUES
('JFK', 'New York'),
('LGA', 'New York'),
('EWR', 'New York'),
('LAX', 'Los Angeles'),
('ORD', 'Chicago'),
('SFO', 'San Francisco'),
('HND', 'Tokyo'),
('NRT', 'Tokyo'),
('KIX', 'Osaka');

--delete from flights;
INSERT INTO flights VALUES
(1, 'JFK', 'HND', '2025-06-15 06:00', '2025-06-15 18:00'),
(2, 'JFK', 'LAX', '2025-06-15 07:00', '2025-06-15 10:00'),
(3, 'LAX', 'NRT', '2025-06-15 10:00', '2025-06-15 22:00'),
(4, 'JFK', 'LAX', '2025-06-15 08:00', '2025-06-15 11:00'),
(5, 'LAX', 'KIX', '2025-06-15 11:30', '2025-06-15 22:00'),
(6, 'LGA', 'ORD', '2025-06-15 09:00', '2025-06-15 12:00'),
(7, 'ORD', 'HND', '2025-06-15 11:30', '2025-06-15 23:30'),
(8, 'EWR', 'SFO', '2025-06-15 09:00', '2025-06-15 12:00'),
(9, 'LAX', 'HND', '2025-06-15 13:00', '2025-06-15 23:00'),
(10, 'KIX', 'NRT', '2025-06-15 08:00', '2025-06-15 10:00');

select * from airports;

create api integration github_int
enabled = true
api_provider = git_https_api
api_allowed_prefixes = ('https://github.com/avuthusiva/retail_db');