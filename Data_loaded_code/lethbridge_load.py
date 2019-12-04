import sys
import urllib.request
import json
import datetime
import uuid
import os
from subprocess import PIPE, Popen
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql import SparkSession, types, functions
from pyspark.sql.functions import udf, month, year


assert sys.version_info >= (3, 5) #make sure we have Python 3.5+
cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Load Lethbridge data') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')


def main():

	url = 'https://moto.data.socrata.com/resource/rsp4-a9ym.json?$limit=400000'#url to retrieve lethbridge dataset from Socrata

	urllib.request.urlretrieve(url, "lethbridge.json")#make a request to fetch the required resource


	#Deleting previous lethbridge.json datasets
	hdfs_path = "/user/klnu/lethbridge.json"
	delete = Popen(["hadoop", "fs", "-rm", "-r", hdfs_path ])
	delete.communicate()

	#print("Deleted")


	#copying the newly loaded data to hdfs
	file_path = "/home/klnu/lethbridge.json"
	hdfs_path = os.path.join(os.sep, 'user', 'klnu', "lethbridge.json")
	put = Popen(["hadoop", "fs", "-put", file_path, hdfs_path], stdin=PIPE, bufsize=-1)
	put.communicate()


	#Reading data from json file and making required selections from dataframe	
	crime_data = spark.read.json("lethbridge.json", multiLine=True)
	crime_df = crime_data.select(crime_data['incident_datetime'],crime_data['address_1'],crime_data['city'],crime_data['latitude'],crime_data['longitude'],crime_data['hour_of_day'],crime_data['parent_incident_type']).cache()

	#Typecasting columns
	crime_df = crime_df.withColumn("lat", crime_df["latitude"].cast(DoubleType()))
	crime_df = crime_df.withColumn("long", crime_df["longitude"].cast(DoubleType()))
	crime_df = crime_df.withColumn("hour", crime_df["hour_of_day"].cast(IntegerType()))

	#Defining and using an UDF to generate unique keys
	uuid_udf = udf(lambda : str(uuid.uuid4()), StringType())
	crime_df = crime_df.withColumn("uuid", uuid_udf())

	#Extracting year,month from incident_datetime
	crime_df = crime_df.withColumn("year",year(crime_df['incident_datetime']))
	crime_df = crime_df.withColumn("month",month(crime_df['incident_datetime']))

	#Adding one as occurences 
	crime_df = crime_df.withColumn("count", functions.lit(1)).cache()


	#Getting crime_type data (generic crime type table for this project) from cassandra cluster
	crime_type_df = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'crime_type', keyspace = 'crimepred').load()

	#Joining two dataframes based on crime_types
	final_df = crime_df.join(crime_type_df, crime_df['parent_incident_type'] == crime_type_df['sub_type'])

	#Selecting required features and aliasing few columns ass per requirement
	final_df = final_df.select(final_df['city'],final_df['type'].alias('crime_type'),final_df['uuid'],final_df['count'],final_df['sub_type'].alias('crimesub_type'),final_df['hour'],final_df['lat'],final_df['long'],final_df['month'],final_df['address_1'].alias('neighbourhood'),final_df['year'])


	#print(final_df.show())
	#print(final_df.count())


	#Appending the base_table(table where crime data for other cities are stored) with lethbridge dataframe
	final_df.write.format("org.apache.spark.sql.cassandra").options(table = 'base_table', keyspace = 'crimepred').mode('append').save()


if __name__ == '__main__':
	main()
