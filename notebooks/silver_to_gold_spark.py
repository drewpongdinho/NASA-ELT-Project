from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType, StringType
from pyspark.sql.functions import col, when, udf

silver_dir = dbutils.fs.ls("/mnt/silver")

def find_newest_file(base_path=silver_dir):
    """
    Finds latest file in silver directory based on modification time
    Returns the full path of the file, which can be used to read in correct data
    """
    try: 
        newest_file = max(silver_dir, key=lambda x: x.modificationTime)
        return newest_file.path
    except Exception as e:
        print(f"Error getting latest file: {e}")
        return None

#Read in silver df from bronze. 
df_silver = spark.read.format("csv").option("header", "true").load(find_newest_file())

# Chain transformations from silver -> gold to enrich dataset

# Fix type error, cast to float in silver df before chaining
df_gold = df_silver.withColumn("miss_distance_astronomical",F.col("miss_distance_astronomical").cast("float"))

# Close approach category based on miss_distance_astronomical
def classify_close_approach(distance):
    if distance <= 0.01:
        return "Very Close"
    elif distance <= 0.05:
        return "Close"
    elif distance <= 0.2:
        return "Moderate"
    else:
        return "Distant"

# Pass function through udf (user defined function) in order for spark to apply functions directly in the df
classify_close_approach_udf = F.udf(classify_close_approach, StringType())
df_gold = df_gold.withColumn("close_approach_category",classify_close_approach_udf(F.col("miss_distance_astronomical")))

# Average size of asteroids in kilometers
df_gold = df_gold.withColumn("average_km", (F.col("km_max") + F.col("km_min")) / 2)

# Asteroid size category
def classify_size_category(size):
    if size < 1:
        return "Small"
    elif size <= 10:
        return "Medium"
    else:
        return "Large"

classify_size_category_udf = udf(classify_size_category, StringType())
df_gold = df_gold.withColumn("size_category", classify_size_category_udf(F.col("average_km")))

# Time to impact (miss_distance_kilometers / km_per_hour in km/s)
df_gold = df_gold.withColumn("time_to_impact", F.col("miss_distance_kilometers") / (F.col("km_per_hour") / 3600))

# Extract year and month from date
df_gold = df_gold.withColumn("approach_year", F.year(F.to_date(F.col("date17"), "yyyy-MM-dd")))
df_gold = df_gold.withColumn("approach_month",F.month(F.to_date(F.col("date17"), "yyyy-MM-dd")))

# Distance relative to Earth's radius (~6371 km)
df_gold = df_gold.withColumn("miss_distance_relative", F.col("miss_distance_kilometers") / 6371)

# Orbiting body category
def classify_orbiting_body(body):
    if body in ["Earth", "Moon"]:
        return "Near-Earth"
    elif body == "Mars":
        return "Mars-Crossing"
    else:
        return "Outer Solar System"

classify_orbiting_body_udf = udf(classify_orbiting_body, StringType())
df_gold = df_gold.withColumn("orbiting_body_category", classify_orbiting_body_udf(F.col("orbiting_body")))

# Clean asteroid name
def clean_name(name):
    import re
    return re.sub(r"[^a-zA-Z0-9 ]", "", name)

clean_name_udf = udf(clean_name, StringType())
df_gold = df_gold.withColumn("asteroid_name_cleaned", clean_name_udf(F.col("name")))

# Data source
df_gold = df_gold.withColumn("data_source",F.lit("NASA"))

# Add hazard_score column
df_gold = df_gold.withColumn(
    "hazard_score",
    when(
        (col("miss_distance_astronomical").cast("string") > 0) & 
        (col("is_potentially_hazardous_asteroid") == "True"),
        (1 / col("miss_distance_astronomical").cast("string")) * col("absolute_magnitude_h").cast("float") / 100
    ).otherwise(0)
)

# Add field with current timestamp (last updated)
df_gold = df_gold.withColumn("record_last_updated",F.current_timestamp())

#Write data to gold container
df_gold.write.mode('overwrite').csv("/mnt/gold/gold_level",header=True)