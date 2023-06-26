# Databricks notebook source
# MAGIC %md
# MAGIC 1. Problem statement  - Get the Details of the order placed by the customer on 2014 January 1st
# MAGIC 2. Compute the monthly customer Revenue
# MAGIC
# MAGIC #SparkSession->The entry point to the Spark SQL.
# MAGIC #SparkSession.builder()->It gives access to Builder API that we used to configure session
# MAGIC #SparkSession.master(local)->It sets the Spark Master URL to connect to run locally.
# MAGIC #SparkSession.appname()->Is sets the name for the application.
# MAGIC #SparkSession.getOrCreate()->If there is no existing Spark Session then it creates a new one otherwise use the existing one.
# MAGIC

# COMMAND ----------

#Reading files from local system
#The text files containing customer details, order details, and order item details
%fs
ls dbfs:/FileStore/tables/sales

# COMMAND ----------

# MAGIC %md
# MAGIC Reading files as csv and creating three dataframes with custom schema

# COMMAND ----------

customer_df = spark.read.csv('dbfs:/FileStore/tables/sales/part_00000.txt',schema="""customer_id INT,customer_fname STRING,customer_lname STRING,customer_email STRING,customer_password STRING,customer_street STRING,customer_city STRING,customer_state STRING,customer_zipcode INT""")

# COMMAND ----------

customer_df.show(2)

# COMMAND ----------

orders_df = spark.read.csv('dbfs:/FileStore/tables/sales/part_00000__1_.txt', schema="""order_id INT,order_date DATE,order_customer_id INT,order_status STRING""")

# COMMAND ----------

orders_df.show(2)

# COMMAND ----------

order_items_df=spark.read.csv('dbfs:/FileStore/tables/sales/part_00000__2_.txt',schema="""order_item_id INT,order_item_order_id INT,order_item_product_id INT,order_item_quantity INT,order_item_subtotal FLOAT,order_item_product_price FLOAT""")

# COMMAND ----------

order_items_df.show(2)

# COMMAND ----------

# Inital analysis on two tables before moving to the actual comptation on all three tables
# joining customer and orders tables and running select statement to check order status of customers
customers_orders_df = customer_df.join(orders_df, customer_df['customer_id']==orders_df['order_customer_id'])
customers_orders_df.show(2)

# COMMAND ----------

customers_orders_df.select('customer_id','order_id','order_date','order_status').orderBy('customer_id').show(10)

# COMMAND ----------

#considating customer order details as one column in struct format
from pyspark.sql.functions import struct
customers_orders_df.select('customer_id',struct('order_id','order_date','order_status').alias('order_details')).orderBy('customer_id').show(10)

# COMMAND ----------

customer_orders_struct = customers_orders_df.select('customer_id',struct('order_id','order_date','order_status').alias('order_details')).orderBy('customer_id')
customer_orders_struct.show(2)

# COMMAND ----------


#performing group by operations on customer id
from pyspark.sql.functions import collect_list
final_df = customer_orders_struct.groupBy('customer_id').agg(collect_list('order_details').alias('order_details')).orderBy('customer_id')
final_df.show(2)



# COMMAND ----------

#Writing as json to local system
final_df.coalesce(1).write.json('dbfs:/FileStore/tables/sales/final')

# COMMAND ----------

# MAGIC %md
# MAGIC Performing actual computation on all the three tables to address problem statement

# COMMAND ----------

customer_details = customer_df.join(orders_df,customer_df['customer_id']==orders_df['order_customer_id']).\
    join(order_items_df,orders_df['order_id']==order_items_df['order_item_order_id'])
customer_details.show(10)

# COMMAND ----------

#denormalization of the final table and store as json file
denorm_df = customer_details. \
select('customer_id','customer_fname','customer_lname','customer_email','order_id','order_date','order_status',struct('order_item_id','order_item_product_id','order_item_subtotal').alias('order_item_details')). \
groupBy('customer_id','customer_fname','customer_lname','customer_email','order_id','order_date','order_status'). \
agg(collect_list('order_item_details').alias('order_item_details')). \
orderBy('customer_id').\
    select('customer_id','customer_fname','customer_lname','customer_email',struct('order_id','order_date','order_status','order_item_details').alias('order_details')). \
groupBy('customer_id','customer_fname','customer_lname','customer_email'). \
agg(collect_list('order_details').alias('order_details')). \
orderBy('customer_id')

# COMMAND ----------

denorm_df.coalesce(1).write.json('dbfs:/FileStore/tables/sales/denorm')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/sales/denorm

# COMMAND ----------

json_df = spark.read.json('dbfs:/FileStore/tables/sales/denorm/part-00000-tid-1855166144804322879-946ba105-ff69-4a39-8dc6-b5d6a8181ef8-101-1-c000.json')

# COMMAND ----------

#Performing explode operation to check orders placed on 2014 january

from pyspark.sql.functions import explode
json_df.select('customer_id','customer_fname',explode('order_details').alias('order_details')).\
    filder('order_details.order_date like "2014-01-01"').\
        orderBy('customer_id').\
            select('customer_id','customer_fname','order_details.order_id','order_details.order_date','order_details.order_status').show(10)

# COMMAND ----------

#flatten the json file to check the revenue generated by each customer
flatten=json_df.select('customer_id','customer_fname',explode('order_details').alias('order_details')). \
select('customer_id','customer_fname',col('order_details.order_date').alias('order_date'),col('order_details.order_id').alias('order_id'),col('order_details.order_status').alias('order_status'),explode('order_details.order_item_details').alias('order_item_details')). \
select('customer_id','customer_fname','order_date','order_id','order_status','order_item_details.order_item_id','order_item_details.order_item_product_id','order_item_details.order_item_subtotal')

# COMMAND ----------

from pyspark.sql.functions import to_date
from pyspark.sql import Row
from pyspark.sql.functions import sum as _sum

flatten.select('customer_id','customer_fname',col("order_date"),to_date(col("order_date"),"yyyy-MM-dd").alias("order_date_converted"),'order_status','order_item_subtotal'). \
filter("order_status IN ('COMPLETE','CLOSED')"). \
groupBy('customer_id','customer_fname',date_format('order_date_converted','yyyy-MM').alias('order_month')). \
agg(_sum('order_item_subtotal').alias('Revenue')). \
orderBy('order_month'). \
show()
