from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim
from pyspark.sql.functions import col , udf
from pyspark.sql.functions import initcap
from pyspark.sql.functions import split

import logging
import sys
import pandas as pd 

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


def process_immigration(spark,input_bucket, output_bucket):
    '''
    Processing Immigration Data 

    Arguments:
        spark         = Spark Cluster
        input_bucket  = S3 Bucket name - Input 
        output_bucket = S3 Bucket name - Output 
    '''

    ## Loading Data 

    df = spark.read.parquet('{}data/*.parquet'.format(input_bucket)) 


    ## Loading Dicts from Bucket

    # Load Port Dict 
    df_port=spark.read.json('{}data/i94prtl.json'.format(input_bucket)).toPandas()
    df_port=spark.createDataFrame(df_port.transpose().reset_index()\
        .rename(columns={'index':'i94port',0:'Port'}))

    # Load Origin Dict 
    df_country=spark.read.json('{}data/i94cntyl.json'.format(input_bucket)).toPandas()
    df_country=spark.createDataFrame(df_country.transpose().reset_index().\
        rename(columns={'index':'i94cit',0:'Origin_Country'}))
    
    # Load Travelmode Dict
    df_mode=spark.read.json('{}data/i94model.json'.format(input_bucket)).toPandas()
    df_mode=spark.createDataFrame(df_mode.transpose().reset_index().\
        rename(columns={'index':'i94mode',0:'Travelmode'}))

    # Load Destination Dict 
    df_dest=spark.read.json('{}data/i94addrl.json'.format(input_bucket)).toPandas()
    df_dest=spark.createDataFrame(df_dest.transpose().reset_index().\
        rename(columns={'index':'i94addr',0:'Destination'}))
    
    # Load Visa Dict
    df_visa=spark.read.json('{}data/i94visa.json'.format(input_bucket)).toPandas()
    df_visa=spark.createDataFrame(df_visa.transpose().reset_index().\
        rename(columns={'index':'i94visa',0:'Visa'}))


    ## Joining Data 

    # Join Port 
    df=df.join(df_port, df.i94port==df_port.i94port, how='left').drop(df_port.i94port)
    # Join Country 
    df=df.join(df_country, df.i94cit==df_country.i94cit, how='left').drop(df_country.i94cit)
    # Join Travelmode
    df=df.join(df_mode,df.i94mode==df_mode.i94mode, how='left').drop(df_mode.i94mode)
    # Join Destination 
    df=df.join(df_dest,df.i94addr==df_dest.i94addr, how='left').drop(df_dest.i94addr)
    # Join Visa Data 
    df=df.join(df_visa,df.i94visa==df_visa.i94visa, how='left').drop(df_visa.i94visa)

    ## Cleaning Data

    # Dropping Column 
    col_to_drop=['visapost','occup','entdepu','count','admnum','matflag','dtaddto','insnum','fltno','i94cit','i94mode','i94addr','i94visa']
    df=df.drop(*col_to_drop)

    # Strip Whitespaces
    df_clean=df.withColumn('Port',trim(col('Port')))

    # Clean Invalid Ports 
    df_port_clean=df_clean.filter(~col('Port').contains('No PORT Code'))\
        .filter(~col('Port').contains('Collapsed'))\
            .filter(col('Port').isNotNull())

    # Invalid Country Codes
    df_country_clean=df_port_clean.filter(~col('Origin_Country').contains('INVALID'))\
        .filter(~col('Origin_Country').contains('Collapsed'))\
            .filter(col('Origin_Country').isNotNull())
    
    # Remove Null-Values in Travlemode
    df_travelmode_clean=df_country_clean.filter(col('Travelmode').isNotNull())

    # Remove Null-Values in Destination
    df_destination_clean=df_travelmode_clean.filter(col('Destination').isNotNull())
    
    # Get City Names #
    
    df_city=df_destination_clean.withColumn("City", initcap(split(col("Port"),',')[0])).drop('Port')

    # Quality Check 

    if df_city.count()<1:
        raise AssertionError('Error in ETL; Immigration ROW-Count is to low!')
    else:
        logging.info('Quality Check succeded')

    ## Writing Data 

    df_city.write.parquet('{}processed/cleaned_immigration'.format(output_bucket),mode='overwrite')

    print('Data cleaned and saved')

def main():
    '''
    Main function. Runs all above 
    '''

    spark=create_spark()
    input_bucket=sys.argv[1]
    output_bucket=sys.argv[2]
    process_immigration(spark,input_bucket,output_bucket)

if __name__ =="__main__":
    main()

