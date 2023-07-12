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
