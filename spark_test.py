from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Cron Spark Test") \
    .getOrCreate()

# Create a simple DataFrame
data = [("John", 30), ("Jane", 25), ("Doe", 40)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame contents
df.show()

# Perform a simple operation like counting rows
count = df.count()
print(f"Row count: {count}")

# Save the result to a file
with open("/Users/alexanderhubbard/projects/Duraflamengo/logs/spark_output.txt", "w") as file:
    file.write(f"Row count: {count}\n")

# Stop the Spark session
spark.stop()
