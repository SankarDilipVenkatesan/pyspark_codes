# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Looking into spark sql commands and parquet files
# MAGIC RDD: 
# MAGIC 1. Immutable
# MAGIC 2. Falut tolerance
# MAGIC 3. Distributed
# MAGIC 4. Lazy evaluation
# MAGIC
# MAGIC Dataset and Dataframe:
# MAGIC 1. Dataset: Strongly types RDD with fixed datatypes
# MAGIC 2. Dataframe: Dataset with names Columns and datatypes basically tables
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC #unix download command
# MAGIC wget -N https://catallaxyservices.com/media/ml-25m/movies.csv
# MAGIC wget -N https://catallaxyservices.com/media/ml-25m/ratings.csv
# MAGIC wget -N https://catallaxyservices.com/media/ml-25m/links.csv
# MAGIC wget -N https://catallaxyservices.com/media/ml-25m/tags.csv

# COMMAND ----------

#checking the files in linux directory
%sh ls -ltr *.csv
pwd

cat /databricks/driver/ratings.csv |head -2

# COMMAND ----------

#Similar to hdfs cp command (from local to hdfs)
dbutils.fs.cp("file:///databricks/driver/movies.csv", "/FileStore/tables/ml-25m/bronze/movies.csv")
dbutils.fs.cp("file:///databricks/driver/links.csv", "/FileStore/tables/ml-25m/bronze/links.csv")
dbutils.fs.cp("file:///databricks/driver/tags.csv", "/FileStore/tables/ml-25m/bronze/tags.csv")
dbutils.fs.cp("file:///databricks/driver/ratings.csv", "/FileStore/tables/ml-25m/bronze/ratings.csv")

# COMMAND ----------

#check the record
#display(dbutils.fs.ls("/FileStore/tables/ml-25m/bronze/"))
dbutils.fs.head("/FileStore/tables/ml-25m/bronze/ratings.csv")

# COMMAND ----------

#reading the file using fucntion and storing in dataframe
def read_file(file,file_type='csv',delimiter=',',infer_schema='True',header = 'True'):
    return spark.read.format(file_type).option('header',header).option('inferSchema',infer_schema).option('sep',delimiter).load(file)

file_movies='/FileStore/tables/ml-25m/bronze/movies.csv'
file_links='/FileStore/tables/ml-25m/bronze/links.csv'
file_ratings='/FileStore/tables/ml-25m/bronze/ratings.csv'
file_tags='/FileStore/tables/ml-25m/bronze/tags.csv'

dbutils.fs.head(file_ratings)
movies = read_file(file_movies)
links = read_file(file_links)
ratings = read_file(file_ratings)
tags = read_file(file_tags)

# COMMAND ----------

#checking the schema nd records
movies.printSchema()
display(ratings.head(2))

# COMMAND ----------

#now lets work on the movies file using spark sql by creating temp view 
movies.createOrReplaceTempView('Movie')
spark.sql("select movieId, title, split(genres,'\\\\|') as genres from Movie").show(2,False)
#spark.sql("select * from movie limit 5").show(20,False)

# COMMAND ----------

movies = spark.sql("select movieid,title, split(genres,'\\\\|') as genres from movie")
movies.write.mode('overwrite').parquet("/FileStore/tables/ml-25m/silver/movies/")

# COMMAND ----------

#similarly for tags and ratings
tags.createOrReplaceTempView('tag')
ratings.createOrReplaceTempView('rating')

spark.sql("select * from tag").show(2,False)
spark.sql("select * from rating").show(2,False)

# COMMAND ----------

#selecting yhe records for tags and raitings and save as parquet file
tags = spark.sql("select userid, movieid, from_unixtime(timestamp) as data_added from tag")
ratings = spark.sql("select userid, movieid, from_unixtime(timestamp) as data_added from rating")

tags.write.mode('overwrite').parquet("/FileStore/tables/ml-25m/silver/tags/")
ratings.write.mode('overwrite').parquet("/FileStore/tables/ml-25m/silver/ratings/")

# COMMAND ----------

#display the files in sliver and bronze folder both local and cluster
display(dbutils.fs.ls('/FileStore/tables/ml-25m/bronze/'))
display(dbutils.fs.ls('/FileStore/tables/ml-25m/silver/movies'))

# COMMAND ----------

#save as parquet file
movies = spark.read.format('parquet').load('/FileStore/tables/ml-25m/silver/movies/')
ratings = spark.read.format('parquet').load('/FileStore/tables/ml-25m/silver/ratings/')


# COMMAND ----------

#extracting only year from movie title using regex
movies.show(2,False)

from pyspark.sql.functions import regexp_extract,col
#movies.withColumn("year", regexp_extract(col('title'),"\(([^()]+))$", 1"));
                                         
date_reg = r'([A-Z][a-z]* [0-9]{1,2}(?:-(?:[A-Z][a-z]* )?[0-9]{1,2})?)'
movies['Date'] = movies['title'].str.extract(date_reg, expand=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #Problem statements
# MAGIC 1. Number of ratings
# MAGIC 2. Number of 5-star ratings
# MAGIC 3. Number of <1 start ratings
# MAGIC 4. Total number of start
# MAGIC 5. Average ratings

# COMMAND ----------

#creating temp view and validating
movies.createOrReplaceTempView('movie')
ratings.createOrReplaceTempView('rating')

# COMMAND ----------

spark.sql("select * from rating").show(2, False)
spark.sql("select * from movie").show(2, False)

# COMMAND ----------

#number of ratings
spark.sql("select count(*) as Number_of_ratings from rating").show()

# COMMAND ----------

#Number of 5-star ratings and < 1 start rating
spark.sql("select sum(case when rating = 5.0 then 1 else 0 end) as number_of_5_star from rating").show()
spark.sql("select sum(case when rating < 1.0 then 1 else 0 end) as number_of_1_star from rating").show()

# COMMAND ----------

#Sum of ratings
spark.sql("select sum(rating) as Total_stars from rating").show()

# COMMAND ----------

#Average ratings
spark.sql("select case when count(*) = 0 then null else cast(sum(rating)/count(*) as decimal(3))end as avg_rating from rating").show()

# COMMAND ----------

movies = spark.sql("select
                   m.movieid,m.title,m.genres,
                   count(*) as number_of_ratings,
                   sum(case when r.rating=5.0  then 1 else 0 end) as num_of_5_star,
                   sum(case when r.rating<=1.0 then 1 else 0 end) as num_of_1_star,
                   sum(r.ratings) as total_ratings,
                   case when count(*)=0 then NULL else case(sum(r.ratings)/count(*) as decimal(3)) end as avg_ratings 
                   from movies m left outer join ratings r on (m.movieid = r.movieid) group by m.movieid,m.title,m.genres
                   ")



# COMMAND ----------

# MAGIC %md
# MAGIC Exploding genres
# MAGIC

# COMMAND ----------

spark.sql("select movieid,explode(genres) as genres from movies").show()
