# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview of working on diabetes dataset
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/diabetes.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp = "diabetes_csv"

df.createOrReplaceTempView(temp)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from diabetes_csv

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "diabetes_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming column names in the table

# COMMAND ----------

#create table using spark sql
%sql
create table diabetisfrm as
select pregnancies,
'plasma glucose' as glucose,
'blood pressure' as bloodpressure,
'triceps skin thickness' as skinthickness,
insulin, bmi,
'diabetes pedigree' as pedigree,
age, diabetes 
from diabetes_csv


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diabetisfrm limit 2

# COMMAND ----------

# MAGIC %md
# MAGIC Reading file as csv
# MAGIC

# COMMAND ----------

#reading file as csv
sdf = spark.read.csv(file_location,header=True,inferSchema=True,sep=',')
sdf.show()
sdf.printSchema()

# COMMAND ----------

sdf.count()

# COMMAND ----------

countByAge = sdf.groupby('age').count()
countByAge.show()

# COMMAND ----------

#spark filter operation
filterDF = sdf.filter(sdf.age>30).sort('blood pressure')
filterDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC SQL build for the dataframe

# COMMAND ----------

sdf.createOrReplaceTempView('table_test')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_test sort by age

# COMMAND ----------

#lazy evaluation - does not perform any operation until an action is called
dfsubset = spark.sql('select * from diabetisfrm where diabetes =1').collect()
display(dfsubset)

# COMMAND ----------

#adding a new column
sparkDF = sdf.withColumn('NewAge',sdf['age'])
#sparkDF.show()

sparkDF_afterdrop = sparkDF.drop('age')
sparkDF_afterdrop.show()

# COMMAND ----------

# MAGIC %md
# MAGIC RDD
# MAGIC 1. Resilient distibuted dataset
# MAGIC 2. fault tolerent collection of elements do work in parallel
# MAGIC 3. Immutable
# MAGIC 4. Lazy evaluation - spark will only operate when forced to which means transformations are not done unitl you call an Action and transformations create a new rdd from an existing rdd
# MAGIC
# MAGIC Transformations:
# MAGIC 1. Apply logic to columns
# MAGIC 2. map - pass each element through a function
# MAGIC 3. filter - select required elements
# MAGIC 4. sample - return subset of data
# MAGIC
# MAGIC Action:
# MAGIC 1. Execute pending transormations
# MAGIC 2. count - return number of elements
# MAGIC 3. reduce - return an aggregation
# MAGIC 4. collect - return result of the driver
# MAGIC 5. take - required subset of data
# MAGIC

# COMMAND ----------

sc

# COMMAND ----------

spark

# COMMAND ----------

#RDD
file_rdd = '/FileStore/tables/hamlet.txt'
rdd = sc.textFile(file_rdd)

rdd.take(2)

# COMMAND ----------

#file count example with map and reduce function where map is a transformation and reduce is an action
linelengths = rdd.map(lambda s: len(s))
#linelengths.collect() #which is a action

totallength = linelengths.reduce(lambda a,b : a+b)
print(totallength)

# COMMAND ----------

#create transformation using parallelize
my_rdd = sc.parallelize([1,2,3,4,5])
my_rdd.collect()
my_rdd.count()

# COMMAND ----------

#creating a seperate function using map and flatmap (always see a new rdd is created from an existing rdd) 
def map1(lines):
    lines = lines.lower()
    lines = lines.split()
    return lines
map_rdd = rdd.map(map1)
#map_rdd.collect()

flat_map = rdd.flatMap(map1)
#flat_map.collect()

# COMMAND ----------

#display elements in the rdd which are not present in skip words
skip_words = ['to','the','of']
rdd_filter = rdd.filter(lambda x:x not in skip_words)
rdd_filter.collect()

# COMMAND ----------

#word count example
count = rdd.flatMap(lambda x: x.split())
map_words = count.map(lambda x: (x,1))
reduce_words = map_words.reduceByKey(lambda x,y: x+y).sortByKey()
reduce_words.take(10)

