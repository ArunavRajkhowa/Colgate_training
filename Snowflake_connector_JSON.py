import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Connect to Snowflake
con = snowflake.connector.connect(
   user='Tonzu',
   password='Snowflake@091287',
   account='ft33439.ap-south-1.aws',
   database='ANALYTICS',
   schema='PUBLIC',
   warehouse='TRANSFORMING' 
)


# JSON FILE
 #Load the JSON file into Snowflake #"D:\\Coding_Projects\\Colgate_training\\data.json"
df_json = pd.read_json("D:\\Coding_Projects\\Colgate_training\\data.json")
print(df_json.to_string())
print("*"*50)

# create table
cursor = con.cursor()
cursor.execute("create or replace  table temp_test (json_data  variant);")

# Define the custom format
cursor.execute("""
    create or replace file format my_json_format
    type = 'JSON'
    strip_outer_array = true
     ;
 """)

# creating temporary internal_stage

cursor.execute("""
    create or replace stage json_temp_int_stage
  file_format = my_json_format;
 """)


# load json to internal stage
cursor.execute("put file://D:\\Coding_Projects\\Colgate_training\\data.json @json_temp_int_stage;")


# Copy the data into Target Table
cursor.execute('''copy into temp_test
    from  @json_temp_int_stage/data.json
    on_error = 'skip_file'
   ;''')

# print the content from the table
cursor.execute("select * from temp_test")

print('-------------------------------------------------')
print("Result of show tables query:")
print(cursor.fetchall())




