from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
import json

with open("parameters.txt") as data:
    conn_params = json.load(data)
    
session = Session.builder.configs(conn_params).create()
session.table("EMP").group_by(col("DEPTNO")).agg(sum(col("SAL")).alias("SUM_SAL"),
                                                 round(avg(col("SAL")),2).alias("AVG_SAL"),
                                                 max(col("SAL")).alias("MAX_SAL"),
                                                 min(col("SAL")).alias("MIN_SAL")
                                                 ).show()
session.close()