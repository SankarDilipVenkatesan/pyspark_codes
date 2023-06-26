# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Testing.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

#Another way of reading files
df = spark.read.format("csv").option("inferSchema","True").option("header","True").option("sep",",").load(file_location)
df.show()

# COMMAND ----------

# Create a view or table
#temp_table_name = "Testing_csv"
df.createOrReplaceTempView("temp_table_name")
#%sql

#/* Query the created temp table in a SQL cell */

spark.sql("select * from temp_table_name").show()

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "Testing_csv"

#df.write.format("parquet").saveAsTable(permanent_table_name)
#df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. pySpark Dataframe
# MAGIC 2. Reading the dataset
# MAGIC 3. Checking dataset
# MAGIC 4. Selecting column index
# MAGIC 5. Adding and dropping columns
# MAGIC

# COMMAND ----------

#creating spark session as the entry point
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('DataFrame').getOrCreate()

# COMMAND ----------

#different ways of reading a dataset with and without schema
#df_pyspark1 = spark.read.option('header','True').csv(file_location).show()
df_pyspark = spark.read.option('header','True').csv(file_location,inferSchema=True).show()

# COMMAND ----------

#check the schema
df_pyspark.printSchema()

# COMMAND ----------

#another way of accessing dataset 
pyspark_data = spark.read.csv(file_location, header =True, inferSchema= True)
pyspark_data.show()

#So finally accessed the dataset and stored in a dataframe

# COMMAND ----------

#selecting a particular column amd many columns
pyspark_data.select('Name').show()
pyspark_data.select(['Name','Age']).show()


# COMMAND ----------

#Adding columns in dataframe
pyspark_data.withColumn('Age After 2 years',pyspark_data['Age']+2).show()

# COMMAND ----------

#Working with columns
pyspark_data = pyspark_data.withColumn('Experience',pyspark_data['Age']/2) 
pyspark_data = pyspark_data.withColumn('Age after 2 years',pyspark_data['Age']+2) 

pyspark_data.show()

# COMMAND ----------

#dropping columns
pyspark_data = pyspark_data.drop('Age after 2 years')
pyspark_data.show()

# COMMAND ----------

#rename columns
pyspark_data.withColumnRenamed ('Name','New Name').show()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Drop columns and rows based on how, threshold and subset
# MAGIC 2. Handling missing values

# COMMAND ----------

file = "/FileStore/tables/Testing1.csv" 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practice').getOrCreate()

df = spark.read.csv(file,header=True,inferSchema = True)
df.show()

# COMMAND ----------

#drop null values
df.na.drop().count()

# COMMAND ----------

#customized drop method based on how 
#2 options 1.any and 2.how
df.na.drop(how='all').show()

# COMMAND ----------

#drop based on threshold means atleast few NA values  should be present based on the threshold value
df.na.drop(how='any',thresh= 3).show()

# COMMAND ----------

df.na.drop(how='any',subset=['Salary']).show()
 
df.na.drop(how='any',subset=['Experience']).show()
    

# COMMAND ----------

#Handling missing values
df.na.fill('Fill').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Filter operations

# COMMAND ----------

df.filter('salary<3000').show()

# COMMAND ----------

#spark filter condition
df.filter('salary<3000').select(['Name','Age']).show()

# COMMAND ----------

df.filter((df['salary']<3000) | (df['salary'] >3800)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC GroupBy and aggregate function

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Aggregate').getOrCreate()

file_3 = "/FileStore/tables/Testing2.csv"

# COMMAND ----------

df = spark.read.csv(file_3,header = 'True', inferSchema = 'True')

# COMMAND ----------

df.show()
df.printSchema()

# COMMAND ----------

#group by to find max salary
df.groupBy('Name').sum().show()

#df.groupBy('Name').max().show()

# COMMAND ----------

#total aggreagate value
df.agg({'salary':'sum'}).show()
