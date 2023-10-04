from pyspark.sql.functions import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
import pandas as pd
import matplotlib.pyplot as plt

def ETL_1_day(path, name):
    # read and create data for 1 day. 
    file_list = os.listdir(f'{path}/{name}')
    parquet_file = [file_name for file_name in file_list if file_name.endswith(".parquet")]
    df = spark.read.parquet(f'{path}/{name}/{parquet_file[0]}')
    df = df.withColumn('Month', lit(name[5:6]))
    return df

def ETL_all_day(folder_path):
    folder_list = os.listdir(folder_path)
    # read data 1 day
    df = ETL_1_day(path = folder_path, name = folder_list[0])
    # read data 30 days
    for file in folder_list[1:]:
        df1 = ETL_1_day(path = folder_path, name = file)
        df = df.union(df1)
        df = df.cache()
    # filter out searching action of valid user_id and keywords
    df = df.filter((col('action') == 'search') & (col('user_id').isNotNull()) & (col('keyword').isNotNull()))
    return df

def most_search(df, month):
    #find out the most-searched keyword
    df = df.filter(col('Month') == month)
    df = df.select('user_id', 'keyword', 'Month')
    df = df.groupBy('user_id', 'keyword', 'Month').count()
    df = df.withColumnRenamed('count', 'Total_search').orderBy('Total_search', ascending= False)
    window = Window.partitionBy('user_id').orderBy(col('Total_search').desc())
    df = df.withColumn('Rank', row_number().over(window))
    df = df.filter(col('Rank') == 1)
    df = df.withColumnRenamed('keyword','Most_Search')
    df = df.select('user_id','Most_Search', 'Month')
    return df



def main():
    folder_path = 'D:\\test\\log_search'
    df = ETL_all_day(folder_path)
    df_t6 = most_search(df, month=6)
    df_t7 = most_search(df, month=7)

    #import 1000 words, prepare for mapping
    print('prepare to import mapping data')
    key_dict_pd_t6 = pd.read_excel('D:\\test\\python_project\\cleaned_data\\sample_df_t6_2.xlsx')
    key_dict_t6 = spark.createDataFrame(key_dict_pd_t6)
    key_dict_pd_t7 = pd.read_excel('D:\\test\\python_project\\cleaned_data\\sample_df_t7_1.xlsx')
    key_dict_t7 = spark.createDataFrame(key_dict_pd_t7)
    print('import done')
	
    print('preparing final df')
    #change column's name for clarity
    df_t6 = df_t6.join(key_dict_t6,'Most_search','inner').select('user_id','Most_search','Category')
    df_t6 = df_t6.withColumnRenamed('Most_search','Most_Search_t6').withColumnRenamed('Category','Category_t6')
    
    #change column's name for clarity
    df_t7 = df_t7.join(key_dict_t7,'Most_search','inner').select('user_id','Most_search','Category')
    df_t7 = df_t7.withColumnRenamed('Most_search','Most_Search_t7').withColumnRenamed('Category','Category_t7')
    
    df_final = df_t6.join(df_t7, "user_id",'inner')

    #create a new column specifying the change in user's most favorite category
    df_final = df_final.withColumn('Category_change', when(col('Category_t6')==col('Category_t7'),'Unchanged').otherwise(concat(df_final['Category_t6'],lit('-'),df_final['Category_t7'])))
    df_final = df_t6.join(df_t7, "user_id",'inner')
    print('final dfs prepared')
    
    #mysql's credential and loading final data 
    USER = 'root'
    PASSWORD = '1'
    HOST = 'localhost'
    PORT = '3306'
    DB_NAME = 'data_engineering'
    URL = 'jdbc:mysql://' + HOST + ':' + PORT + '/' + DB_NAME
    DRIVER = "com.mysql.cj.jdbc.Driver"
    df_final.write.format("jdbc") \
            .option('driver', DRIVER) \
            .option('url', URL) \
            .option('dbtable', 'bigdata') \
            .mode('append') \
            .option('user', USER) \
            .option('password', PASSWORD) \
            .save()
    print('loading complete')

spark = SparkSession.builder.getOrCreate()    
main()
    