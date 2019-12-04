#Writing the training data into cassandra cluster to train the machine learning model

import sys
import uuid
import os
from subprocess import PIPE, Popen
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql import SparkSession, types, functions
from pyspark.sql.functions import udf, month, year


assert sys.version_info >= (3, 5) #make sure we have Python 3.5+
cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Load the training Data') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')


def main():
	
	#Import the crime data from cassandra cluster
	crime_df = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'base_table', keyspace = 'crimepred').load()

	#Import crime code data from cassandra cluster
	crime_code_df = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'crime_code', keyspace = 'crimepred').load()
	
	crime_df = crime_df.withColumn("lat", functions.round(crime_df["lat"].cast(DoubleType()),4))	

	crime_df = crime_df.withColumn("long", functions.round(crime_df["long"].cast(DoubleType()),4))	

	crime_df = crime_df.filter(crime_df['lat'].isNotNull()) #Remove null lat values

	crime_df = crime_df.filter((crime_df['lat'] != 0.0) | (crime_df['long'] != 0.0)) #remove lat and long values that are zero

	crime_df = crime_df.select(crime_df['lat'],crime_df['long'],crime_df['year'],crime_df['crime_type'],crime_df['count']).cache()

	result_df = crime_df.join(crime_code_df, crime_df['crime_type'] == crime_code_df['type_crime'])#Join the crime data with crime code data


	#Adding unique primary keys
	uuid_udf = udf(lambda : str(uuid.uuid4()), StringType())
	result_df = result_df.withColumn("uuid", uuid_udf())


	#Extracting final values
	final_df = result_df.select(result_df['uuid'], result_df['lat'], result_df['long'], result_df['year'], result_df['code'], result_df['count'])

	final_df.write.format("org.apache.spark.sql.cassandra").options(table = "training_table",keyspace = "crimepred").mode('append').save()

	#print(final_df.show())

if __name__ == '__main__':
	main()



