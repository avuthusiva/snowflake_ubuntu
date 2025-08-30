from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
import json

with open('config.json') as json_file:
    config = json.load(json_file)
session = Session.builder.configs(config).create()
data1 = [[1,'siva',33],[2,'reddy',35]]
data2 = [[3,'meera',33],[4,'potdar',34]]
df1 = session.create_dataframe(data1, schema=['id','name','age'])
df2 = session.create_dataframe(data2, schema=['id','name','age'])
df3 = df1.union(df2).sort('id')
df4 = df1.intersect(df2)
df5 = df1.minus(df2)
df3.show()
df4.show()
df5.show()
session.close()