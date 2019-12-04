
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys

def main(key,tables):
        crime = spark.read.format("org.apache.spark.sql.cassandra").options(table=tables, keyspace=key).load()

        crimes = crime.cache()
        crimes.registerTempTable("base_table")
        #To popuate y-o-y crime count for each city based on its crime type
        crimes_yoy = spark.sql("select city,crime_type,year,sum(count) as count from base_table group by city,crime_type,year order by city,crime_type,year")
        crimes_yoy.write.format("org.apache.spark.sql.cassandra").options(table='crimes_yoy', keyspace=key).mode('append').save()

        #To populate the crime type and its total count for years from 2014 to 2019
        crimes_typ = spark.sql("select crime_type,sum(count) as count from base_table where year <= 2019 and year > 2013 group by crime_type")
        crimes_typ.write.format("org.apache.spark.sql.cassandra").options(table='crimes_typ', keyspace=key).mode('append').save()

        #To populate the hour of the day in which these counts occur more frequently.Remove incidents from Calgary,Edmonton as they do not report accurate day
        crimes_hr = spark.sql("select year,hour,sum(count) as count from base_table where city not in ('Calgary','Edmonton') and year <= 2019 and year > 2013 group by year,hour order by year,hour")
        crimes_hr.write.format("org.apache.spark.sql.cassandra").options(table='crimes_hr', keyspace=key).mode('append').save()

        #To populate whether majority of crime incidents occur in day or night. Removed Edmonton as the hour feature is missing for this city.
        crimes_dl = spark.sql("select *,CASE WHEN hour >= 6 and hour <= 18 THEN 'day' else 'night' end as daylight from base_table where city not in ('Edmonton')")
        crimes_dl.registerTempTable("crime_dl")
        crimes_dlagg = spark.sql("select city,crime_type,daylight,sum(count) as count from crime_dl group by daylight,crime_type,city order by daylight")
        crimes_dlagg.write.format("org.apache.spark.sql.cassandra").options(table='crimes_dl', keyspace=key).mode('append').save()

        #To populate the average crime statistic by season.
        crime_ss = spark.sql("select city,crime_type,sum(count) as sum,"
                                           "CASE WHEN month in (12,1,2) THEN 'Winter' "
                                           "WHEN month in (3,4,5) THEN 'Spring' "
                                           "WHEN month in (6,7,8) THEN 'Summer' "
                                           "else 'Fall' end as seasons "
                             "from base_table where year <= 2019 and year > 2013 group by city,crime_type,month ")
        crime_ss.registerTempTable("crime")
        crime_ssagg = spark.sql("select city,crime_type,seasons,avg(sum) as avgsum from crime group by seasons,crime_type,city order by seasons")
        crime_ssagg.write.format("org.apache.spark.sql.cassandra").options(table='crimes_ss', keyspace=key).mode('append').save()


if __name__ == '__main__':
        conf = SparkConf().setAppName('crimeagg')
        sc = SparkContext(conf=conf)
        cluster_seeds = ['199.60.17.32', '199.60.17.65']

        spark = SparkSession.builder.appName('crimeagg').config('spark.cassandra.connection.host',','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
        sc.setLogLevel('WARN')
        assert sc.version >= '2.4'  # make sure we have Spark 2.4+
        assert sys.version_info >= (3, 5)
        key = 'crimepred'
        tables = 'base_table'
        main(key,tables)

