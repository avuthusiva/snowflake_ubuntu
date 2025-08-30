use role accountadmin;
use warehouse my_warehouse;
use database my_db;
create schema procedure_practice;
use schema procedure_practice;
set a ='meera';
select $a;
call system$wait(5);
create or replace procedure pr_hello(p_name string)
returns string
language sql
as
declare
    name string default 'siva';
begin
    return 'hello '|| :p_name;
end;
call pr_hello($a);

set name = 'siva reddy';
select $name;

execute immediate
$$
begin
    return 'hello '|| $name;
end
$$;

set a = 10;
select $a;

execute immediate
$$
declare 
    a int default 20;
    c int;
begin
    return :a + $a;
end;
$$;

set table_name = 'my_db.my_schema.emp';

execute immediate
$$
declare
    cnt int;
begin
    select count(*) into :cnt from table($table_name);
    return :cnt;
end;
$$;

set database_name = 'my_db';
set schema_name = 'procedure_practice';
use database identifier($database_name);
use schema identifier($schema_name);

execute immediate
$$
declare 
    str string := '';
begin
    for i in 1 to 10
    loop
        str := str || ' ' || i;
    end loop;
    return str;
end;
$$;

execute immediate
$$
declare
    str string := '';
    i int := 1;
begin
    while (i<= 10)
    loop
        str := :str || ' ' || :i;
        i := :i + 1;
    end loop;
    return :str;
end;
$$;

begin
    let cur cursor for select * from my_db.my_schema.emp;
    for rec in cur
    loop
        null;
    end loop;
    return 'done';
end;

execute immediate
$$
declare
    cur cursor for select * from my_db.my_schema.emp;
    out_data resultset;
begin
    out_data := (execute immediate 'select * from my_db.my_schema.emp where deptno = 10');
    return table(out_data);
end;
$$;

CREATE OR REPLACE PROCEDURE sp_get_ddl( var_db_name VARCHAR, var_schema_name VARCHAR)
RETURNS TABLE(DDL VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
  cursor_sql VARCHAR DEFAULT 'SELECT table_name, table_type FROM '||var_db_name||
  '.information_schema.tables
    WHERE table_schema = '''||var_schema_name||''' and table_type= ''BASE TABLE'' ORDER BY table_name';
  cursor_resultset RESULTSET DEFAULT (EXECUTE IMMEDIATE :cursor_sql);
  table_cursor CURSOR FOR cursor_resultset;
  my_sql VARCHAR;
  my_union_sql VARCHAR;
  res RESULTSET;
  counter NUMBER DEFAULT 1;
BEGIN
  FOR var in table_cursor DO
    my_sql := 'SELECT GET_DDL(''TABLE'','''||var.table_name||''')';
    IF(counter=1) THEN
      my_union_sql := :my_sql;
    ELSE
      my_union_sql := :my_union_sql || ' UNION ALL ' || :my_sql;
    END IF;
    counter := counter + 1;
  END FOR;
  res := (EXECUTE IMMEDIATE :my_union_sql);
  RETURN table(res);
END;
$$
;

use schema my_db.my_schema;
call sp_get_ddl('MY_DB','MY_SCHEMA');  

select * from information_schema.tables where table_catalog = 'MY_DB' and table_schema = 'MY_SCHEMA' 
and table_type = 'BASE TABLE' order by table_name;

create or replace procedure sp_get_tables(var_db_name varchar,var_schema_name varchar)
returns table(table_name varchar)
language sql
as
declare
    cursor_sql varchar := 'select table_name from information_schema.tables where table_catalog = 
    ''' || var_db_name || ''' and table_schema = ''' || var_schema_name || ''' and
    table_type = ''BASE TABLE'' order by table_name';
    res resultset := (execute immediate :cursor_sql);
    table_cursor cursor for res;
    my_sql varchar;
    my_union_sql varchar;
    counter number default 1;
begin
    for var in table_cursor
    loop
        my_sql := 'select get_ddl(''TABLE'',''' || var.table_name || ''')';
        if (counter = 1)
        then
            my_union_sql := :my_sql;
        else
            my_union_sql := :my_union_sql || ' union all ' || :my_sql;
        end if;
        counter := counter + 1;
    end loop;
    res := (execute immediate :my_union_sql);
    return table(res);
end;

call sp_get_tables('MY_DB','MY_SCHEMA');

execute immediate
$$
declare
    sql := 'drop table my_table';
begin
    execute immediate :sql;
    return 'table dropped';
exception
    when STATEMENT_ERROR
    then
        return object_construct('error_message', 'Table does not exist', 'error_code', SQLERRM,
        'error_number', SQLSTATE);
    when other
    then
        return 'Exception in others'; 
end;
$$;

