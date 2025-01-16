from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when

# Step 1: Create a Spark session
spark = SparkSession.builder \
    .appName("Random DataFrame Transformations") \
    .getOrCreate()

# Step 2: Generate a random DataFrame
# Create a DataFrame with random values for demonstration
data = [(i, round(rand().cast("double").getItem(0) * 100, 2)) for i in range(1, 11)]
columns = ["id", "random_value"]

# Create the DataFrame
df = spark.createDataFrame(data, schema=columns)

print("Original DataFrame:")
df.show()

# Step 3: Perform some transformations
# a. Add a new column categorizing the `random_value` as "high", "medium", or "low"
transformed_df = df.withColumn(
    "category",
    when(col("random_value") > 70, "high")
    .when(col("random_value") > 30, "medium")
    .otherwise("low")
)

# b. Filter rows with "high" values
filtered_df = transformed_df.filter(col("category") == "high")

# c. Add a new column with random values multiplied by 10
final_df = filtered_df.withColumn("scaled_value", col("random_value") * 10)

print("Transformed DataFrame:")
final_df.show()

# Step 4: Stop the Spark session
spark.stop()

