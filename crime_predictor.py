import sys

from pyspark.sql import SparkSession

assert sys.version_info >= (3, 5) 
assert sys.version_info >= (3, 5) #make sure we have Python 3.5+
cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Load Victoria data') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')

#Importing all dependencies for training the machine learning model
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, GBTParams, GBTRegressionModel
from pyspark.ml import Pipeline

#Importing all data type utilities
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql import SparkSession, types, functions
from pyspark.sql.functions import udf, month, year

def main():
	
	crime_df = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'training_table', keyspace = 'crimepred').load()
	
	crime_df = crime_df.filter(crime_df['lat'].isNotNull())

	crime_df = crime_df.filter((crime_df['lat'] != 0.0) | (crime_df['long'] != 0.0))

	crime_df = crime_df.select(crime_df['lat'],crime_df['long'],crime_df['year'],crime_df['code'],crime_df['count'])

	#print(crime_df.show())

	#print(crime_df.printSchema())


	train,validation = crime_df.randomSplit([0.60,0.40])#Creating a split in avaliable data

	train = train.cache()

	validation = validation.cache()

	#print(train.show())

	query = "select * from __THIS__"

	transformer = SQLTransformer(statement = query)

	features = VectorAssembler(inputCols = ['lat','long','year','code'], outputCol = 'input_features')


	#Creating a GBT Regressor model for training the data
	predictor = GBTRegressor(featuresCol = 'input_features', labelCol =  'count', maxDepth = 5, maxIter = 100, seed = 50, stepSize = 0.1)

	gbt_pipeline = Pipeline(stages = [transformer,features,predictor])

	gbt_model = gbt_pipeline.fit(train)

	output_predictions = gbt_model.transform(validation)

	gbt_r2_evaluator = RegressionEvaluator(predictionCol = 'prediction',labelCol = 'count', metricName = 'rmse')

	gbt_r2_score = gbt_r2_evaluator.evaluate(output_predictions)

	#	print(gbt_r2_score)

	gbt_model.write().overwrite().save("predictor_model")#Writing the model to disk




if __name__ == '__main__':
	main()






