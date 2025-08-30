from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
import json

with open('config.json') as f:
    config = json.load(f)
session = Session.builder.configs(config).create()
emp_df = session.table("EMP")
dept_df = session.table("DEPT")
inner_join = dept_df.join(emp_df, dept_df.DEPTNO == emp_df.DEPTNO, "inner")
inner_join.show()
left_join = dept_df.join(emp_df, dept_df.DEPTNO == emp_df.DEPTNO, "left")
left_join.show()
right_join = dept_df.join(emp_df, dept_df.DEPTNO == emp_df.DEPTNO, "right")
right_join.show()
full_join = dept_df.join(emp_df, dept_df.DEPTNO == emp_df.DEPTNO, "full")
full_join.show()
session.close()