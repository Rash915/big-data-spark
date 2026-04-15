from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("BigDataProject").getOrCreate()

# Read dataset
df = spark.read.csv("sample.csv", header=True, inferSchema=True)

# Show data
df.show()

# Filter high-value products
df_filtered = df.filter(df["price"] > 100)

# Group by category and calculate revenue
result = df_filtered.groupBy("category").sum("revenue")

# Show results
result.show()

# Sort results
result.orderBy("sum(revenue)", ascending=False).show()

# Save output
result.write.mode("overwrite").csv("output", header=True)
