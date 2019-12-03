from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import isnan
import zipfile
import sys
import pandas as pd
import urllib.request
import utm
import uuid

# Schema Definition for incoming data
def schema_def():
    schema = types.StructType([
        types.StructField('subtype', types.StringType()),
        types.StructField('year', types.IntegerType()),
        types.StructField('month', types.IntegerType()),
        types.StructField('day', types.IntegerType()),
        types.StructField('hour', types.IntegerType()),
        types.StructField('minute', types.IntegerType()),
        types.StructField('hundred_block', types.StringType()),
        types.StructField('neighbourhood', types.StringType()),
        types.StructField('X', types.FloatType()),
        types.StructField('Y', types.FloatType()),
    ])
    return schema

# Main para that handles all preprocessing for Vancouver City
def main(link,key):
    crime_schema = schema_def()
    # API request is made to retrieve data from Vancouver Open Data
    urllib.request.urlretrieve(link, "Vancouver.zip")
    compressed_file = zipfile.ZipFile('Vancouver.zip')
    csv_file = compressed_file.open('crimedata_csv_all_years.csv')

    pd_crimes = pd.read_csv(csv_file)

    # Creation of Spark DataFrame
    df_crime_init = spark.createDataFrame(pd_crimes,schema=crime_schema).cache()
    # Tagging City to Vancouver
    df_crime_init = df_crime_init.withColumn("city",functions.lit("Vancouver"))
    # UDF to apply UUID for entire dataframe
    genuuid = functions.udf(lambda : str(uuid.uuid4()))
    df_crime_init = df_crime_init.withColumn("uuid",genuuid()).cache()

    # Changing NaN values to 0 for numeric  columns & Filtering those rows that has not latitude and longitude
    df_crime_init = df_crime_init.na.fill(0)
    df_crime  = df_crime_init.where((df_crime_init["X"] > 0) | (df_crime_init["Y"] > 0))

    # Conversion of UTM co-orindates to Latitude and Longitude
    utm_udf_x = functions.udf(lambda x, y: utm.to_latlon(x, y, 10, 'U')[0].item(), types.DoubleType())
    utm_udf_y = functions.udf(lambda x, y: utm.to_latlon(x, y, 10, 'U')[1].item(), types.DoubleType())
    df_crime = df_crime.withColumn('lat', utm_udf_x(functions.col('X'), functions.col('Y')))
    df_crime = df_crime.withColumn('long', utm_udf_y(functions.col('X'), functions.col('Y')))

    # Creating a new dataframe to store those records that does not have co-ordinates. We would need this for further study
    df_crime_nan = df_crime_init.where(df_crime_init["X"] == 0.0)
    df_crime_nan = df_crime_nan.withColumn("lat",functions.lit(0.0))
    df_crime_nan = df_crime_nan.withColumn("long",functions.lit(0.0))
    # Union of both dataframes
    df_crime_full = df_crime.union(df_crime_nan)

    # Calling Cassandra DB crime_type to tag subtypes to one common type
    crimepred = spark.read.format("org.apache.spark.sql.cassandra").options(table='crime_type', keyspace=key).load()
    crimepred.registerTempTable("crimetype")
    df_crime_full.registerTempTable("crime")
    df_table = spark.sql("select c.city,t.type as crime_type,c.uuid,c.subtype as crimesub_type,c.hour,c.lat,c.long,c.month,c.neighbourhood,c.year from crime c left join crimetype t on c.subtype = t.sub_type")
    df_table = df_table.withColumn('count',functions.lit(1))

    df_na = df_table.where(df_table["crime_type"].isNull()).show()
    #df_crime_nan.show()
    print(df_table.count())

    # Loading of data into base_table
    df_table.write.format("org.apache.spark.sql.cassandra").options(table='base_table', keyspace=key).mode('overwrite').save()

if __name__ == '__main__':
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('vanc crime set').config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    #sc.addPyFile("utm.py")
    link = "http://geodash.vpd.ca/opendata/crimedata_download/crimedata_csv_all_years.zip"
    key = 'crimepred'
    main(link,key)

