import uuid

import pandas as pd
from geopy.extra.rate_limiter import RateLimiter

from pyspark.sql.functions import udf, concat
from sodapy import Socrata
from geopy.geocoders import Nominatim
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import SparkSession, types, functions

cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Spark Cassandra toronto load') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


@udf('double')
def lat_udf(x):
    geolocator = Nominatim(user_agent="edmn", timeout=10000)
    geocoder = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    location = geocoder(x)
    
    if location is None:
        return 0.0
    else:
        return location.latitude

@udf('double')
def long_udf(x):
    geolocator = Nominatim(user_agent="edmn", timeout=10000)
    geocoder = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    location = geocoder(x)
    
    if location is None:
        return 0.0
    else:
        return location.longitude


def main():
    #making soda API calls
    client = Socrata("dashboard.edmonton.ca", None)
    results = client.get("xthe-mnvi", limit=10000000)

    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)

    # Dataframe schema defenition
    crime_schema = types.StructType([
        types.StructField('neighbourhood', types.StringType()),
        types.StructField('month', types.StringType()),
        types.StructField('quarter', types.StringType()),
        types.StructField('year', types.StringType()),
        types.StructField('crimesub_type', types.StringType()),
        types.StructField('count', types.StringType()),
    ])

    #creating dataframe
    crime_df = spark.createDataFrame(results_df, schema=crime_schema)
    crime_df = crime_df.select(crime_df['neighbourhood'], crime_df['crimesub_type'], crime_df['year'].cast(IntegerType()),
                    crime_df['month'].cast(IntegerType()), crime_df['count'].cast(IntegerType()))

    #adding city, hour and uuid column to the dataframe
    crime_df = crime_df.withColumn("city", functions.lit("Edmonton"))
    crime_df = crime_df.withColumn("hour", functions.lit(0))
    uuid_udf = udf(lambda : str(uuid.uuid4()), StringType())
    crime_df = crime_df.withColumn("uuid", uuid_udf())

    # Calling Cassandra DB crime_type to tag subtypes to one common type
    crimepred = spark.read.format("org.apache.spark.sql.cassandra").options(table='crime_type', keyspace='crimepred').load()
    crime_df.registerTempTable("crimedf")
    crimepred.registerTempTable("crimepred")
    crime_df = spark.sql("select a.city, t.type as crime_type, a.uuid, a.crimesub_type, a.neighbourhood, a.year, a.month, a.count, a.hour from crimedf as a left join crimepred as t on a.crimesub_type = t.sub_type")
    crime_df.show()

    #adding a column locality for latitude and longitude generation purpose
    crime_df = crime_df.select(crime_df['*'], concat(crime_df['neighbourhood'], functions.lit(', Edmonton, Canada')).alias("locality"))

    #loading the latitude and longitude table for locality from cassandra
    latlong_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='edmonton_latlong', keyspace='crimepred').load()
    latlong_df.show()

    #joining two tables to get all the latitude and longitude values
    latlong_df.registerTempTable("temp")
    crime_df.registerTempTable("crime")
    crime_df = spark.sql("select a.city, a.crime_type, a.uuid, a.crimesub_type, a.neighbourhood, a.year, a.month, a.hour, b.lat, b.long, a.count, a.locality from crime as a left join temp as b on a.locality = b.locality")

    #removing latitude and longitude that cannot be converted
    crime_df.registerTempTable("crime1")
    crime_df = crime_df.where((crime_df['lat'] != 0.0) & (crime_df['long'] != 0.0))

    #checking for scalability:if a new address comes that is not in cassandra database, using geopy to compute the latitude and longitude
    crime_df.registerTempTable("new_add")
    new_latlong = spark.sql("select a.city, a.crime_type, a.uuid, a.crimesub_type, a.neighbourhood, a.year, a.month, a.hour, a.count, a.locality from new_add as a where isnull(a.lat) and isnull(a.long)")

    if new_latlong.count() > 0:
     #new address has been encountered and latitude and longitude have to be generated for this
     new_latlong = new_latlong.withColumn("lat", lat_udf(new_latlong['locality']))
     new_latlong = new_latlong.withColumn("long", long_udf(new_latlong['locality']))

     #updating the cassandra edmonton_latlong table with new address latitude and longidtude
     new_cassandra = new_latlong.select(new_latlong['locality'], new_latlong['lat'], new_latlong['long']).distinct()
     new_cassandra.write.format("org.apache.spark.sql.cassandra").options(table='edmonton_latlong', keyspace='crimepred').mode('append').save()

     #adding back the new latitude and longitude to the crime df
     crime_df.registerTempTable("crimefinal")
     crime_df = spark.sql("select a.city, a.crime_type, a.uuid, a.crimesub_type, a.neighbourhood, a.year, a.month, a.hour, a.lat, a.long, a.count, a.locality from crimefinal as a where not isnull(a.lat) and not isnull(a.long)")
     crime_df = crime_df.union(new_latlong)
     final_df = crime_df.drop(crime_df['locality'])

    else :
     #no new latitude and longitude encountered
     final_df = crime_df.drop(crime_df['locality'])

    final_df.show()
    #Loading of data into base_table
    final_df.write.format("org.apache.spark.sql.cassandra").options(table='base_table', keyspace='crimepred').mode('append').save()


if __name__ == '__main__':
    main()








