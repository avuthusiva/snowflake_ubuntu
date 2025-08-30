from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
import json
import numpy as np  

with open('config.json') as f:
    config = json.load(f)
    
session = Session.builder.configs(config).create()
@udf(name="sqrt_fun",
     return_type=FloatType(),
     input_types=[IntegerType()],
     replace=True,
     is_permanent=False,
     packages=['numpy'])
def sqrt_fun(x:int) -> float:
    return np.sqrt(x)
df = session.create_dataframe([100,200,300,400,500],schema=["numbers"])
df1 = df.select(col("numbers"),round(sqrt_fun(col("numbers")),2).alias("sqrt"))
df1.show()
session.close()
