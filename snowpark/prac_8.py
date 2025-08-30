from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
import json

with open("config.json", "r") as f:
    config = json.load(f)
session = Session.builder.configs(config).create()
@udf(name="reverse_string", 
     return_type=StringType(),
     input_types=[StringType()],
     replace=True,
     is_permanent=True,
     stage_location='@int_stage_functions/udf')
def reverse_string(s: str) -> str:
    return s[::-1]
df = session.table("emp")
df_rev = df.select(col("ename"),reverse_string(col("ename")).alias("ename_rev"))
df_rev.show()
session.close()