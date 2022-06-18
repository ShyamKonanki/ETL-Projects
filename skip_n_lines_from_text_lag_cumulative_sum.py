#sample data
#This article provides steps to create, start, and monitor a tumbling window trigger. For general information about triggers and the supported types, see Pipeline execution and triggers.
#
#Tumbling window triggers are a type of trigger that fires at a periodic time interval from a specified start time, while retaining state. Tumbling windows are a series of fixed-sized, non-overlapping, and contiguous time intervals. A tumbling window trigger has a one-to-one relationship with a pipeline and can only reference a singular pipeline. Tumbling window trigger is a more heavy weight alternative for schedule trigger offering a suite of features for complex scenarios(dependency on other tumbling window triggers, rerunning a failed job and set user retry for pipelines). To further understand the difference between schedule trigger and tumbling window trigger, please visit here.
#abc,123,2022-04-11
#abc,234,2022-04-12
#abc,350,2022-04-13
#def,124,2022-04-11
#def,235,2022-04-12
#def,351,2022-04-13

from pyspark.sql.functions import *
from pyspark.sql import Window
rdd = sc.textFile('/mnt/Test/SampleData/*.txt').zipWithIndex()
rdd = rdd.filter(lambda x: x[1] > 2)
df = rdd.map(lambda x:x[0].split(',')).toDF(["name","samples", "date"])
df = df.withColumn("date", to_date(col("date")))
window = Window.orderBy("date").partitionBy("name")
df_final = df.withColumn("lag_date", (col("samples") - lag(col("samples")).over(window))).withColumn("cumulative_sum", sum(col('samples')).over(window))
display(df_final)
