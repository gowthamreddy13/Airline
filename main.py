df=spark.read.json("/FileStore/tables/json/airlines.json")

file_location = "/FileStore/tables/airlines.json"
file_type = "json"

# Read the JSON file as a string column
df = spark.read.text(file_location)

# Filter out rows with corrupt records
corrupt_records_df = df.filter(df.value.contains("_corrupt_record"))

# Count the number of corrupt records
corrupt_records_count = corrupt_records_df.count()

# Show the corrupt records
corrupt_records_df.show(truncate=False)

-------------
display(df, format="table")
-------------------------
df=spark.read.option("multiline", "true").json("/FileStore/tables/json/airlines.json")
------------------------
display(df)
--------------------
from pyspark.sql.types import *
from pyspark.sql.functions import *


#Flatten array of structs and structs
def flatten(df):
   # compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
 # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df
  ----------------------------------------------------------------------
df_flatten = flatten(df)
display(df_flatten)
-----------------------------
