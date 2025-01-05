from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, DoubleType
from pyspark.sql.functions import col, explode, from_json
import os
from datetime import date

storage_account_name = "medallionstorageee"
storage_account_key = "***"

# Ensure that the storage account key is correctly passed in extra_configs

# Mount the bronze container
dbutils.fs.mount(
    source = f"wasbs://bronze@{storage_account_name}.blob.core.windows.net/",
    mount_point = "/mnt/bronze/",
    extra_configs = {
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key
    }
)

# Mount the silver container
dbutils.fs.mount(
    source = f"wasbs://silver@{storage_account_name}.blob.core.windows.net/",
    mount_point = "/mnt/silver/",
    extra_configs = {
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key

    }
)

# Mount the gold container
dbutils.fs.mount(
    source = f"wasbs://gold@{storage_account_name}.blob.core.windows.net/",
    mount_point = "/mnt/gold/",
    extra_configs = {
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key
    }
)

#Read in Bronze df. 
today = date.today().strftime("%Y-%m-%d")
df = spark.read.format("csv").option("header", "true").load(f"dbfs:/mnt/bronze/raw_neo_data_{today}.csv")
#df.show()

from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, DoubleType

# Define the "Close Approach Data" json schema (match API output)
close_approach_data_schema = ArrayType(StructType([
    StructField("close_approach_date", StringType(), True),
    StructField("close_approach_date_full", StringType(), True),
    StructField("epoch_date_close_approach", LongType(), True),
    StructField("relative_velocity", StructType([
        StructField("kilometers_per_second", StringType(), True),
        StructField("kilometers_per_hour", StringType(), True),
        StructField("miles_per_hour", StringType(), True)
    ]), True),
    StructField("miss_distance", StructType([
        StructField("astronomical", StringType(), True),
        StructField("lunar", StringType(), True),
        StructField("kilometers", StringType(), True),
        StructField("miles", StringType(), True)
    ]), True),
    StructField("orbiting_body", StringType(), True)
]))

# Two fields (close_approach, estimated_diameter) in the df contain nested json data, hence a matching schema is required
# for spark to interpret the from_json and explode functions.
# The aim is to have one value per NEO with a field for each of the nested df parameters
# The data can then easily be joined together

# Parse the JSON array
df_parsed_cad = df.withColumn("close_approach_data_parsed",from_json(col("close_approach_data"), close_approach_data_schema))

# Explode the array to flatten each object into its own row
df_exploded_cad = df_parsed_cad.withColumn("close_approach_data_exploded", explode(col("close_approach_data_parsed")))

# Return columns of interest from struct
df_flattened_cad = df_exploded_cad.select(
    col("id"),
    col("close_approach_data_exploded.close_approach_date").alias("date"),
    col("close_approach_data_exploded.close_approach_date_full").alias("date_full"),
    col("close_approach_data_exploded.epoch_date_close_approach").alias("epoch_date"),
    col("close_approach_data_exploded.relative_velocity.kilometers_per_second").alias("km_per_second"),
    col("close_approach_data_exploded.relative_velocity.kilometers_per_hour").alias("km_per_hour"),
    col("close_approach_data_exploded.relative_velocity.miles_per_hour").alias("miles_per_hour"),
    col("close_approach_data_exploded.miss_distance.astronomical").alias("miss_distance_astronomical"),
    col("close_approach_data_exploded.miss_distance.lunar").alias("miss_distance_lunar"),
    col("close_approach_data_exploded.miss_distance.kilometers").alias("miss_distance_kilometers"),
    col("close_approach_data_exploded.miss_distance.miles").alias("miss_distance_miles"),
    col("close_approach_data_exploded.orbiting_body").alias("orbiting_body")
)

# Define the "Estimated Diameter Data" json schema
estimated_diameter_schema = StructType([
    StructField("kilometers", StructType([
        StructField("estimated_diameter_min", DoubleType(), True),
        StructField("estimated_diameter_max", DoubleType(), True)
    ])),
    StructField("meters", StructType([
        StructField("estimated_diameter_min", DoubleType(), True),
        StructField("estimated_diameter_max", DoubleType(), True)
    ])),
    StructField("miles", StructType([
        StructField("estimated_diameter_min", DoubleType(), True),
        StructField("estimated_diameter_max", DoubleType(), True)
    ])),
    StructField("feet", StructType([
        StructField("estimated_diameter_min", DoubleType(), True),
        StructField("estimated_diameter_max", DoubleType(), True)
    ]))
])


# Parse the JSON column using defined schema
df_parsed_ed = df.withColumn("estimated_diameter_parsed",from_json(col("estimated_diameter"), estimated_diameter_schema))

# Return columns of interest from struct
df_flattened_ed = df_parsed_ed.select(
    col("id"),  
    col("estimated_diameter_parsed.kilometers.estimated_diameter_min").alias("km_min"),
    col("estimated_diameter_parsed.kilometers.estimated_diameter_max").alias("km_max"),
    col("estimated_diameter_parsed.meters.estimated_diameter_min").alias("meters_min"),
    col("estimated_diameter_parsed.meters.estimated_diameter_max").alias("meters_max"),
    col("estimated_diameter_parsed.miles.estimated_diameter_min").alias("miles_min"),
    col("estimated_diameter_parsed.miles.estimated_diameter_max").alias("miles_max"),
    col("estimated_diameter_parsed.feet.estimated_diameter_min").alias("feet_min"),
    col("estimated_diameter_parsed.feet.estimated_diameter_max").alias("feet_max")
)

# Sort data to improve RLE compression
df = df.sort(col("date"))
df_flattened_ed = df_flattened_ed.sort(col("date"))
df_flattened_cad = df_flattened_cad.sort(col("date"))

# Join the data together
df_for_silver = (df.join(df_flattened_ed, "id").join(df_flattened_cad,"id"))

#Drop irrelevant fields
df_for_silver = df_for_silver.drop("links", "estimated_diameter","close_approach_data")

#Write data to Silver container // compress partitions to 1 file 
df_for_silver.coalesce(1).write.mode('overwrite').option("header", "true").csv("/mnt/silver/from_bronze/",header=True)

# Locate, move and rename the file to a consistent naming convention

# Create variables to move files from bronze to silver
output_dir = "/mnt/silver/from_bronze/" 
input_dir = "/mnt/silver/"
files = dbutils.fs.ls(output_dir)
actual_filename = [f.name for f in files if f.name.startswith("part-")][0] 

# Create paths to move data
source_path = os.path.join(output_dir, actual_filename)
destination_path = os.path.join(input_dir, f"silver_data_{today}.csv")

# Move data from directory to root
dbutils.fs.mv(source_path, destination_path)

# Keep directory clean with single output file for gold layer
dbutils.fs.rm(output_dir, recurse=True)
