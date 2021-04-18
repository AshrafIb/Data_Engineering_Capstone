from pyspark.sql import SparkSession
from pyspark.sql.functions import col , udf
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


def process_airport(spark,input_bucket, output_bucket):
    '''
    Processing Airport Data 

    Arguments:
        spark         = Spark Cluster
        input_bucket  = S3 Bucket name - Input 
        output_bucket = S3 Bucket name - Output 
    
    '''

    df = spark.read.csv('{}data/airport-codes_csv.csv'.format(input_bucket),header=True)

    df_clean=df.filter(col('iata_code').isNotNull())

    df_clean=df_clean.drop(col('continent'))

    # Quality Check 

    if df_clean.count()<1:
        raise AssertionError('Error in ETL; Airport ROW-Count is to low!')
    else:
        logging.info('Quality Check succeded')

    df_clean.write.csv('{}processed/airport.csv'.format(output_bucket),mode='overwrite',header=True)

    print('Airport Data processed and saved')


def main():
    '''
    Main function. Runs all above 
    '''
    spark=create_spark()
    input_bucket=sys.argv[1]
    output_bucket=sys.argv[2]
    process_airport(spark, input_bucket,output_bucket)

 
if __name__=='__main__':
    main()

