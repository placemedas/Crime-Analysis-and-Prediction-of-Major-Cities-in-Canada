import uuid

import pandas as pd
from geopy.extra.rate_limiter import RateLimiter

from pyspark.sql.functions import udf, concat
from sodapy import Socrata
from geopy.geocoders import Nominatim
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import SparkSession, types, functions


cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Spark Cassandra example') \
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
    client = Socrata("dashboard.edmonton.ca", None)
    results = client.get("xthe-mnvi", limit=10000000)

    # Convert to pandas DataFrame
    results_df = pd.DataFrame.from_records(results)

    # Dataframe schema defenition
    crime_schema = types.StructType([
        types.StructField('neighbourhood', types.StringType()),
        types.StructField('crimesub_type', types.StringType()),
        types.StructField('year', types.StringType()),
        types.StructField('quarter', types.StringType()),
        types.StructField('month', types.StringType()),
        types.StructField('count', types.StringType()),
    ])

    crime_df = spark.createDataFrame(results_df, schema=crime_schema)
    crime_df = crime_df.select(crime_df['neighbourhood']).distinct()
    print(crime_df.count())

    #generating latitude and longitude
    latlong_df = crime_df.select(concat(crime_df['neighbourhood'], functions.lit(', Edmonton, Canada')).alias("locality"))
    latlong_df.show()

    latlong_df = latlong_df.withColumn("lat", lat_udf(latlong_df['locality']))
    latlong_df = latlong_df.withColumn("long", long_udf(latlong_df['locality']))
    latlong_df.show()

    #writing into Cassandra table
    latlong_df.write.format("org.apache.spark.sql.cassandra") \
        .options(table="edmonton_latlong", keyspace="crimepred").mode("append").save()


if __name__ == '__main__':
    main()



