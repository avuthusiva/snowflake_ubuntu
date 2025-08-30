from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
import json

with open('config.json') as f:
    config = json.load(f)
    
session = Session.builder.configs(config).create()
df = session.table("author_details")
df.select(col("details")["isbn"].alias("isbn"),
          col("details")["author"].alias("author"),
          col("details")["title"].alias("title"),
          col("details")["genre"].alias("genre"),
          col("details")["awards"].alias("awards"),
          col("details")["publisher"].alias("publisher"),
          col("details")["publication_year"].alias("publication_year"),
          col("details")["reviews"].alias("reviews")
          ).show()
session.close()