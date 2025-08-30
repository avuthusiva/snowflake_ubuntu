from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
import json

with open("config.json") as f:
    config = json.load(f)

session = Session.builder.configs(config).create()
add_one = udf(lambda x:x+1,return_type=IntegerType(),input_types=[IntegerType()])
data = list(range(1,100))
#print(data)
df = session.create_dataframe(data,schema=["numbers"])
df_new = df.with_column("add_10",add_one(col("numbers"))).select("numbers","add_10").orderBy("numbers")
df_new.show()
session.close()