# SCD TYPE 2 implementation using pyspark dataframes

from pyspark.sql.types import *
from pyspark.sql.functions import *
from functools import reduce
from pyspark.sql import DataFrame

# initial dataset
data = [['Shyam', 'sales', 20], ['Karra', 'HR', 30], ['Uday', 'sales', 40]]
schema = StructType([StructField('name',StringType()), StructField('dept', StringType()), StructField('sal',IntegerType())])
df1 = spark.createDataFrame(data,schema)

# First change dataset
data2 = [['Shyam', 'HR', 20],  ['Uday', 'sales', 30], ['Mahesh', 'Bank', 30]]
df2 = spark.createDataFrame(data2,['name', 'dept', 'sal'])
df_old = df1.join(df2,df1['name'] == df2['name'], 'left').withColumn('start_date',  lit('1990-01-01')).withColumn('end_date', when(df2['name'].isNotNull(), current_date()-5).otherwise(lit(None))).select(df1['name'], df1['sal'], df1['dept'], 'start_date', 'end_date')
df_new = df2.withColumn('start_date',  current_date()-5).withColumn('end_date', lit(None))
df_main = df_old.unionByName(df_new).orderBy('name',desc('start_date'))
df_main.show()

#function to maintain SCD 2
def SCD_type2(df_main, df_incremental):
    df_change_in_data = df_main.join(df_incremental, 'name', 'leftsemi').where("end_date is null").withColumn("end_date", current_date()-1)
    df_no_change_in_data= df_main.join(df_incremental, 'name', 'leftsemi').where("end_date is not null")
    df_new_data = df_incremental.withColumn('start_date',  current_date()).withColumn('end_date', lit(None))
    df = reduce(DataFrame.unionByName, [df_change_in_data,df_no_change_in_data, df_new_data])
    return df

# second change dataset to verify SCD2 function
data3 = [['Shyam', 'HR', 40],  ['Uday', 'sales', 50], ['Mahesh', 'Bank', 40], ['Ramya', 'Bank', 40]]
df3 = spark.createDataFrame(data3,['name', 'dept', 'sal'])

display(SCD_type2(df_main,df3))



