import os 
from datetime import date
today = date.today()

# Locate, move and rename the file to a consistent naming convention

# Create variables to move files below
output_dir = "/mnt/gold/gold_level" 
input_dir = "/mnt/gold/"
files = dbutils.fs.ls(output_dir)
actual_filename = [f.name for f in files if f.name.startswith("part-")][0] 

# Create paths to move data
source_path = os.path.join(output_dir, actual_filename)
destination_path = os.path.join(input_dir, f"gold_data_{today}.csv")

# Move data from directory to root
dbutils.fs.mv(source_path, destination_path)

# Keep directory clean with single output file for gold layer
dbutils.fs.rm(output_dir, recurse=True)

from pyspark.sql.functions import current_timestamp

gold_dir = dbutils.fs.ls("/mnt/gold")

def find_newest_file(base_path=gold_dir):
    """
    Finds latest file in silver directory based on modification time
    Returns the full path of the file, which can be used to read in correct data
    """
    try: 
        newest_file = max(gold_dir, key=lambda x: x.modificationTime)
        return newest_file.path
    except Exception as e:
        print(f"Error getting latest file: {e}")
        return None

#Read in silver df from bronze. 
df_gold = spark.read.format("csv").option("header", "true").load(find_newest_file())

display(df_gold)

# Connection parameters for Azure db
jdbc_hostname = "medallion-db-am.database.windows.net"
jdbc_port = 1433
jdbc_database = "medallion-spark-db-dev"
jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


# SQL Server credentials
username = "***"
password = "***"

# Connection properties
connection_properties = {
    "user": username,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

#Before appending the data to Azure SQL table, create new ingestion column - useful timestamp
df_gold = df_gold.withColumn("ingestion_timestamp", current_timestamp())

# Write DataFrame to Azure SQL Database
df_gold.write.jdbc(url=jdbc_url, table="gold_level_table", mode="append", properties=connection_properties)

# Unmount containers
dbutils.fs.unmount("/mnt/gold")
dbutils.fs.unmount("/mnt/silver")
dbutils.fs.unmount("/mnt/bronze")