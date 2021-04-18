from pyspark.sql import SparkSession
from pyspark.sql.functions import col , udf
import pyspark.sql.functions as f
from pyspark.sql.functions import year, month, dayofmonth
import datetime
import logging

import sys

def create_spark():
    '''
    Creates Spark Cluster
    
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print('Session created')

    return spark 

def process_weather(spark,input_bucket,output_bucket,past_years=10):
    '''
    Processing Weather Data 

    Arguments: 
        spark = Spark Cluster
        input_bucket = S3 Bucket name - Input 
        output_bucket = S3 Bucket name - Output 
        past_years = Aggregate over the last X -Years 
    '''
    # Load Data
    df = spark.read.csv('{}data/GlobalLandTemperaturesByCity.csv'.format(input_bucket),
                header=True)
    
    # Create Year and Month Colum 
    df=df.withColumn('Month',month('dt'))\
        .withColumn('Year',year('dt'))
    
    # Latest Year
    latest_year=df.groupBy('Year').count().orderBy(col('Year').desc()).collect()[0][0]

    # Year Range 
    list_of_years=[]

    for y in range(past_years):
        list_of_years.append(latest_year-y)
    
    # Filter Year 
    df_latest=df.filter(col('Year').isin(list_of_years))

    # Filter on USA 
    df_clean=df_latest.filter(col('Country')=="United States")

    # Drop Duplicates
    df_clean=df_clean.filter(col('AverageTemperature').isNotNull())\
        .drop_duplicates(subset=['City','Country','Latitude','Longitude'])
    
    # Group by Country 
    df_grp=df_clean.select('City','AverageTemperature','AverageTemperatureUncertainty')\
        .groupBy('City').agg(f.avg('AverageTemperature').alias('City_AverageTemperature'),
        f.avg('AverageTemperatureUncertainty').alias('City_AverageTemperatureUncertainty'))

    # Quality Check 

    if df_grp.count()<1:
        raise AssertionError('Error in ETL; Weather ROW-Count is to low!')
    else:
        logging.info('Quality Check succeded')

    # Write Data 
    df_grp.write.csv('{}processed/weather.csv'.format(output_bucket),mode='overwrite')

def main():
    '''
    Main function. Runs all above
    '''

    spark=create_spark()
    input_bucket=sys.argv[1]
    output_bucket=sys.argv[2]
    process_weather(spark,input_bucket,output_bucket,past_years=10)

if __name__=='__main__':
    main()