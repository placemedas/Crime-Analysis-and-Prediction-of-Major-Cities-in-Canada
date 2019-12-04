
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import concat, col, lit,udf
from sodapy import Socrata
import sys
import uuid


def main(key):
    #SODA API call to calgary open data source
    client = Socrata("data.calgary.ca", None)
    results = client.get("848s-4m4z",limit = 10000000)

    df_crime_set = spark.createDataFrame(results)

    #Fetching of relevant data from fetched data source
    df_crime_set.registerTempTable('crime_init')
    df_crime = spark.sql("select community_name as neighbourhood,category as subtype,cast(count as int),year,month(cast(date as timestamp)) as month,"
                             "hour(cast(date as timestamp)) as hour,geocoded_column.longitude as long,geocoded_column.latitude as lat from crime_init")

    #Tagging records to Calgary city
    df_crime = df_crime.withColumn('city',lit('Calgary'))

    #Generation of UUID as a unique key
    genuuid = udf(lambda : str(uuid.uuid4()))
    df_crime = df_crime.withColumn("uuid",genuuid())

    #Loading crime types
    crimepred = spark.read.format("org.apache.spark.sql.cassandra").options(table='crime_type', keyspace=key).load()
    crimepred.registerTempTable("crimetype")

    #Grouping of many crime types into common group types
    df_crime.registerTempTable("crime")
    df_table = spark.sql("select c.city,t.type as crime_type,c.uuid,c.subtype as crimesub_type,c.hour,c.lat,c.long,c.month,c.neighbourhood,c.year,c.count from crime c left join crimetype t on c.subtype = t.sub_type")

    #Removal of non-matching dummy records that has inappropriate crime data
    df_table_err = df_table.filter(df_table['crime_type'].isNull())
    df_table = df_table.filter(df_table['crime_type'].isNotNull())

    # Loading of data into base_table
    df_table.write.format("org.apache.spark.sql.cassandra").options(table='base_table', keyspace=key).mode('append').save()


if __name__ == '__main__':
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('calgary crime set').config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    key = 'crimepred'
    main(key)

