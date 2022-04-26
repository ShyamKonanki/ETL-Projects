# Find 3 or more consecutive empty seats.

# Method 1 Using lead lag function
from pyspark.sql.functions import *
from pyspark.sql import Window
data = [(1,'N')
,(2,'Y')
,(3,'N')
,(4,'Y')
,(5,'Y')
,(6,'Y')
,(7,'N')
,(8,'Y')
,(9,'Y')
,(10,'Y')
,(11,'Y')
,(12,'N')
,(13,'Y')
,(14,'Y')]
df=spark.createDataFrame(data,['seat_no', 'is_empty'])
window = Window.orderBy("seat_no")
df2 = df.withColumn('previous_1', lag(df.is_empty, 1).over(window)).withColumn('previous_2', lag(df.is_empty, 2).over(window)).withColumn('next_1', lead(df.is_empty, 1).over(window)).withColumn('next_2', lead(df.is_empty, 2).over(window))
df3 = df2.filter((df2['is_empty'] == 'Y') & (((df2["previous_1"] == 'Y') & (df2["previous_2"] == 'Y')) | ((df2["previous_1"] == 'Y') & (df2["next_1"] == 'Y')) | ((df2["next_2"] == 'Y') & (df2["next_1"] == 'Y'))))
display(df3.select("seat_no"))


# Method 2
# using advanced window function

from pyspark.sql.functions import *
from pyspark.sql import Window
window1 = Window.orderBy("seat_no").rowsBetween(-2, 0)
window2 = Window.orderBy("seat_no").rowsBetween(-1, 1)
window3 = Window.orderBy("seat_no").rowsBetween(0, 2)
df2 = df.withColumn('set1', collect_list('is_empty').over(window1)).withColumn('set2', collect_list('is_empty').over(window2)).withColumn('set3', collect_list('is_empty').over(window3))
df3 = df2.withColumn('prev_2', array_remove(df2['set1'], 'N')).withColumn('prev_next', array_remove(df2['set2'], 'N')).withColumn('next_2', array_remove(df2['set3'], 'N'))
display(df3.filter((size(df3['prev_2']) == 3) | (size(df3['prev_next']) == 3) | (size(df3['next_2']) == 3) ).select('seat_no'))
