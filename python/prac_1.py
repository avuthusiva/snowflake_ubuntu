import ibis

ibis.options.interactive = True

snf_conn = ibis.snowflake.connect(
    user = "asvr410",
    password = "RA-one41132025",
    account = "CX76824.ap-southeast-1",
    database = "my_db",
    schema = "my_schema"
)
data = snf_conn.list_tables()
print(data)
emp_data = snf_conn.tables.EMP
print(emp_data.HIREDATE['timestamp'])