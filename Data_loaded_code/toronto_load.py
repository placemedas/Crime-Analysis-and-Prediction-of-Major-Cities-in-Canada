import sys
import urllib.request
import json
import datetime
import uuid
import os
from subprocess import PIPE, Popen

from pyspark.sql.types import IntegerType, StringType

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types, functions
from pyspark.sql.functions import udf

cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Spark Cassandra toronto load') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')


def main():
    #Dataframe schema defenition
    crime_schema = types.StructType([
        types.StructField('Division', types.StringType()),
        types.StructField('Hood_ID', types.LongType()),
        types.StructField('Index_', types.LongType()),
        types.StructField('Lat', types.DoubleType()),
        types.StructField('Long', types.DoubleType()),
        types.StructField('MCI', types.StringType()),
        types.StructField('Neighbourhood', types.StringType()),
        types.StructField('ObjectId', types.LongType()),
        types.StructField('event_unique_id', types.StringType()),
        types.StructField('occurrencedate', types.LongType()),
        types.StructField('occurrenceday', types.LongType()),
        types.StructField('occurrencedayofweek', types.StringType()),
        types.StructField('occurrencedayofyear', types.LongType()),
        types.StructField('occurrencehour', types.LongType()),
        types.StructField('occurrencemonth', types.StringType()),
        types.StructField('occurrenceyear', types.LongType()),
        types.StructField('offence', types.StringType()),
        types.StructField('permisetype', types.StringType()),
        types.StructField('reporteddate', types.LongType()),
        types.StructField('reportedday', types.LongType()),
        types.StructField('reporteddayofweek', types.StringType()),
        types.StructField('reporteddayofyear', types.LongType()),
        types.StructField('reportedhour', types.LongType()),
        types.StructField('reportedmonth', types.StringType()),
        types.StructField('reportedyear', types.LongType()),
        types.StructField('ucr_code', types.LongType()),
        types.StructField('ucr_ext', types.LongType()),
    ])

    url = "https://opendata.arcgis.com/datasets/98f7dde610b54b9081dfca80be453ac9_0.geojson"

    #making the api call
    urllib.request.urlretrieve(url, "test.json")

    #loading the data from the retrieved file as a dictionary
    with open('test.json') as data_file:
        data = json.load(data_file)

    #loading the data into a json file
    with open('data.json', 'w') as f:
        json.dump(data["features"], f)

    #copying to hdfs
    file_path = "/home/kchakola/data.json"
    hdfs_path = os.path.join(os.sep, 'user', 'kchakola', "data.json")
    put = Popen(["hadoop", "fs", "-put", file_path, hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()

    #creating dataframe from the nested json file
    crime_df = spark.read.json("data.json", multiLine=True)
    reqd_df = crime_df.select("properties.*")

    #adding city and uuid column to the dataframe
    reqd_df = reqd_df.withColumn("city", functions.lit("Toronto"))
    uuid_udf = udf(lambda : str(uuid.uuid4()), StringType())
    reqd_df = reqd_df.withColumn("uuid", uuid_udf())

    #udf for converting month name
    month_udf = udf(lambda x: datetime.datetime.strptime(x, '%B').month, IntegerType())
    reqd_df = reqd_df.select(reqd_df['*'], month_udf(reqd_df['reportedmonth']).alias("month"))

    #selecting required columns from the dataframe
    final_df = reqd_df.select(reqd_df['city'], reqd_df['MCI'].alias("crime_type"), reqd_df['uuid'], reqd_df['offence'].alias("crimesub_type"),
                              reqd_df['reportedhour'].alias("hour"), reqd_df['Lat'].alias("lat"), reqd_df['Long'].alias("long"),
                              reqd_df['month'], reqd_df['Neighbourhood'].alias("neighbourhood"), reqd_df['reportedyear'].alias("year"))

    #writing into Cassandra table
    final_df.write.format("org.apache.spark.sql.cassandra") \
        .options(table="base_table", keyspace="crimepred").mode("append").save()


if __name__ == '__main__':
    main()





