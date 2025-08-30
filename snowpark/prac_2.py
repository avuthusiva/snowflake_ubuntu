from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
import json

with open("parameters.txt","r") as data:
    conn_params = json.load(data)
session = Session.builder.configs(conn_params).create()
emp_df = session.table("EMP").filter(col("SAL") > 2000)
#emp_df.show()
#print(emp_df.schema)
print(emp_df.columns)
print(emp_df.count())
print(emp_df.collect())
print(emp_df.printSchema())