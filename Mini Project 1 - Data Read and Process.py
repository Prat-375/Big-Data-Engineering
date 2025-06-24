# Databricks notebook source
# MAGIC %md
# MAGIC Read and Process Data

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CustomerDataProcessing").getOrCreate()
spark

# COMMAND ----------

df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/workspace/default/customers/customers.csv")

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = df.withColumn('registration_date', to_date(col('registration_date'), 'yyyy-MM-dd')).withColumn('is_active', col('is_active').cast('boolean'))

# COMMAND ----------

df.show(5)
df.printSchema()

# COMMAND ----------

df = df.fillna({'city': 'Unknown','state': 'Unknown', 'country': 'Unknown'})

# COMMAND ----------

df.show(5)

# COMMAND ----------

df = df.withColumn('registration_year', year(col('registration_date'))).withColumn('registration_month', month(col('registration_date')))

# COMMAND ----------

df.show(5)

# COMMAND ----------

unique_cities = df.select(countDistinct('city')).collect()
print(unique_cities)
print(unique_cities[0])
print('cities:',unique_cities[0][0])

unique_states = df.select(countDistinct('state')).collect()
print('states:',unique_states[0][0])

unique_countries = df.select(countDistinct('country')).collect()
print('countries:',unique_countries[0][0])

# COMMAND ----------

df.groupby('city').count().orderBy(col('count').desc()).show()

# COMMAND ----------

df.groupBy('state','country').count().orderBy(col('count').desc()).show()


# COMMAND ----------

# pivot Table - Count of active and inactive users per state
df.groupBy('state').pivot('is_active').count().show()

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

window_spec = Window.partitionBy('state').orderBy(col('registration_date').desc())

df = df.withColumn('rank', rank().over(window_spec)).withColumn('dense_rank', dense_rank().over(window_spec)).withColumn('row_number', row_number().over(window_spec))

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.select('name','city','state','rank','dense_rank','row_number').show(5)

# COMMAND ----------

df_recent_customers = df.filter(col('registration_date') >= lit('2023-07-01'))
df_recent_customers.count()

# COMMAND ----------

# oldest and newest customer per city

df.groupBy('city').agg(min('registration_date').alias('oldest'), max('registration_date').alias('newest')).show()

# COMMAND ----------

output_path = '/Volumes/workspace/default/customers'
df.write.mode('overwrite').parquet(output_path)