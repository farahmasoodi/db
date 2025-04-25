# Databricks notebook source
#Load the csv File
df=spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/student_data.csv')
df.display()


# COMMAND ----------

# Loop through each column in the DataFrame
for col_name in df.columns:
    
# Group the data by the current column
    grouped_data = df.groupBy(col_name)
    
#  Count how many times each value appears
    value_counts = grouped_data.count()
    
# Keep only the duplicate values
    duplicate_values = value_counts.filter("count > 1")
    
# Count how many such duplicate values are there
    dup_count = duplicate_values.count()
    
# Display the result
    print(f"{col_name} has {dup_count} duplicate values")

  

# COMMAND ----------

# Drop Duplicate Values
df.dropDuplicates().display()

# COMMAND ----------

# Delete the entire column
df.drop('Age').display()

# COMMAND ----------

student_data = {
    "Name": ['Yash', 'Harshit', 'John', 'Vaibhav', 'Sneha', 'Sanya', 'Ansh', 'Vanya'],
    "Age": [21, 25, None, 26, None, None, 30, None]
}
print(student_data)

# COMMAND ----------

# Create Dataframe
df = spark.createDataFrame(zip(student_data["Name"], student_data["Age"]), ["Name", "Age"])
# Drop rows where Age is None
df.dropna(subset=["Age"]).display()

# Fill None values in Age with 0
df.fillna({"Age": 10}).display()

# COMMAND ----------

# Window Functions

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, sum, col

data = [
    ("Vanya", "Sales", 5000),
    ("Sneha", "IT", 4800),
    ("Sanya", "IT", 5200),
    ("Neha", "IT", 6000),
    ("Arya", "HR", 6500),
    ("Khushi", "HR", 6000),
]
columns = ["Name", "Department", "Salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Define window specification, partition by department, order by salary
window_spec = Window.partitionBy("Department").orderBy(col("Salary").desc())

# Apply window functions
df_with_window = df.withColumn("Row_Number", row_number().over(window_spec)) \
                   .withColumn("Rank", rank().over(window_spec)) \
                   .withColumn("Dense_Rank", dense_rank().over(window_spec)) \
                   .withColumn("Lag_Salary", lag("Salary", 1).over(window_spec)) \
                   .withColumn("Lead_Salary", lead("Salary", 1).over(window_spec)) \
                   .withColumn("Running_Sum", sum("Salary").over(window_spec))
df.display() 
# Show result
df_with_window.show(truncate=False)

