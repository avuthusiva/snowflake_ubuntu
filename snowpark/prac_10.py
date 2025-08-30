from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
from snowflake.snowpark.files import *
import json

with open("config.json") as f:
    connection_parameters = json.load(f)
session = Session.builder.configs(connection_parameters).create()

@udf(name="files_in_stage", 
     is_permanent=False, 
     replace=True,
     packages=["snowflake-snowpark-python"], 
     session=session,
     return_type=StringType(),
     input_types=[ArrayType(StringType())])
def files_in_stage(file_path):
    file_path = f"@{file_path}"
    files = session.sql("list {}".format(file_path)).collect()
    file_list = [file['name'] for file in files]
    return str(file_list)

files_in_stage(session,"int_stage")
session.close()