''' 
@author: Altair
    
Pip:
    pyspark
'''

# Import necessary modules
from pyspark.sql import SparkSession
import csv
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()

def get_data():
    
    df = pd.read_csv('data.csv')

    return df

def startpy():
    
    df = spark.createDataFrame(get_data())

    df.createOrReplaceTempView("tact_data")
    df_result =  spark.sql("SELECT * \
                            from tact_data \
                            where Area<800")

    df_result.toPandas().to_csv('static/results.csv')

    print("khalaas")


if __name__ == '__main__':
    startpy()
