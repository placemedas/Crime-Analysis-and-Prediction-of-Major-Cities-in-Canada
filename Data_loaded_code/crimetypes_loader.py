
import re
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types

cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def main():

    # Scheme defenition
    crime_schema = types.StructType([
        types.StructField('sub_type', types.StringType()),
        types.StructField('type', types.StringType()),
    ])

    #Data Frames creation
    crimetypes_df = spark.read.csv("crime_types.csv", schema=crime_schema)

    #writing into Cassandra table
    crimetypes_df.write.format("org.apache.spark.sql.cassandra") \
        .options(table="crime_type", keyspace="crimepred").mode("append").save()


if __name__ == '__main__':
    main()



