from pyspark.sql import SparkSession
from pyspark.sql.functions import col , udf
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as f

import logging
import sys
import os 

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

def process_demographic(spark, input_bucket,output_bucket):
    '''
    Processing Demographic Data 

    Arguments: 
        spark = Spark Cluster
        input_bucket  = S3 Bucket name - Input 
        output_bucket = S3 Bucket name - Output 
    '''
    ## Load Data

    data_input=os.path.join(input_bucket,'data/us-cities-demographics.csv')

    df = spark.read.options(delimiter=';')\
        .csv(data_input,header=True)
    
    ## Creating Grp. Dataframe 

    # Groupby and Pivot
    df_grp=df.groupBy('City','State').pivot('Race').agg(f.sum('Count').cast(IntegerType()))
    # Create Key Column
    df_grp=df_grp.withColumn('StateCity',f.concat(col('State'),f.lit('_'),col('City')))
    # Drop State and City 
    col_to_drop=['City','State']
    df_grp=df_grp.drop(*col_to_drop)
    # Fill Nulls with zero
    df_grp=df_grp.fillna(0)

    ## Joining 

    # Create Join key origin df
    df=df.withColumn('StateCity',f.concat(col('State'),f.lit('_'),col('City')))
    # Join per key 
    df=df.join(df_grp,df.StateCity==df_grp.StateCity,how='left').drop(df_grp.StateCity)

    # Dropping Columns
    col_to_drop=['StateCity','Race','Count']
    df=df.drop(*col_to_drop)

    # Quality Check 

    if df.count()<1:
        raise AssertionError('Error in ETL; Demographic ROW-Count is to low!')
    else:
        logging.info('Quality Check succeded')

    # Write to csv 
    df.write.csv('{}processed/demographics.csv'.format(output_bucket),mode='overwrite',header=True)



def main():
    '''
    Main function. Runs all above
    '''
    spark=create_spark()
    input_bucket=sys.argv[1]
    output_bucket=sys.argv[2]
    process_demographic(spark,input_bucket,output_bucket)

if __name__=='__main__':
    main()

