import os
import json

file_path = "/home/sivaa/Desktop/work/snowflake/snowpark"
for root, dirs, files in os.walk(file_path):
    for file in files:
        if file.endswith(".py"):
            print(os.path.join(root,file))
            file_name = os.path.join(root,file)
            data = open(file_name,"r")
            print(list(data))
