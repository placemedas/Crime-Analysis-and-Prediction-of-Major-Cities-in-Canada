from flask import Flask, render_template, request,jsonify
from geopy.geocoders import Nominatim
import pandas as pd
from pyspark.sql import SparkSession, functions, types
from pyspark.ml import PipelineModel


spark = SparkSession.builder.appName('weather tomorrow').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

#from Get_Data import get_data, build_connection,run_sql
#import random
#import string
#from OpenSSL import SSL

#Schema for spark dataframe that shall be used for prediction
crime_schema = types.StructType([
    types.StructField('Lat', types.FloatType()),
    types.StructField('Long', types.FloatType()),
    types.StructField('Code', types.IntegerType()),
    types.StructField('Type', types.StringType())
])

type_schema = types.StructType([
    types.StructField('Code', types.IntegerType()),
    types.StructField('Type', types.StringType())
])



#First, create a flask class object we use to run our program
app = Flask(__name__)



#set the route to the first place you go. So when I go to the site http://0.0.0.0:500 I will render my Index.html template
@app.route('/')
def home():
    return render_template("home.html")


@app.route('/', methods=['POST'])
def my_form_post():

    # Obtain the inputs from web-page request
    text = request.form['text']

    # Retrieve location based on address provided
    try:
        geolocator = Nominatim(user_agent="Geopy")
        location = geolocator.geocode(text)
        # Obtaining the negihbourhood address
        neighbourhood = location.address
        # Rounding off the latitude and longitude to 3 decimal points
        lat = round(location.latitude, 3)
        long = round(location.longitude, 3)
    except AttributeError:
        error = 'The provided address does not exist in Maps. Please recheck your address'
        return render_template('home.html',values=error)

    # List of cities
    cities = ['Vancouver','Ottawa','Victoria','Calgary','Lethbridge','Edmonton','Toronto']
    if any(city in neighbourhood for city in cities) == True:

        # Fixed set of pre-defined parent crime types .Hence stored as a dictionary
        crimetypes = {1: 'Assault',2: 'Drugs',3: 'Robbery',4: 'Theft Over',5: 'Liquor',6: 'Break and Enter',7: 'Vehicle Collision',8: 'Social Disorder',9: 'Missing Person',10: 'Auto Theft',11: 'False Alarm',12: 'Kidnap',13: 'Property Crime'}
        crimedf = pd.DataFrame(list(crimetypes.items()),columns=['Code', 'Type'])
        crimesp = spark.createDataFrame(crimedf, schema=type_schema)

        # Empty List to create latitude and longitude
        latitude = []
        longitude = []

        # Creating Latitudes and Longitude for 1 km ahead.
        lati = lat
        longi = long
        for i in range(0,10):
            lati = lati + 0.001
            longi = longi + 0.001
            latitude.append(lati)
            longitude.append(longi)

        # Creating Latitude and Longitude 1km behind
        latr = lat
        longr = long
        for i in range(0,10):
            latr = latr - 0.001
            longr = longr - 0.001
            latitude.append(latr)
            longitude.append(longr)

        # Combine latitude and longitude into a tuple
        tuplelist = list(zip(latitude, longitude))
        df = pd.DataFrame(tuplelist, columns=['Lat', 'Long'])

        # Performing a cartesian product. This dataframe shall have Lat longs and the possibility of all crime types to be predicted
        product = (
            df.assign(key=1)
                .merge(crimedf.assign(key=1), on="key")
                .drop("key", axis=1)
        )

        #Passing the pandas dataframe into Spark for model prediction
        crimepred = spark.createDataFrame(product, schema=crime_schema)
        crimepred = crimepred.select(crimepred["Lat"],crimepred["Long"],crimepred["Code"])
        crimepred = crimepred.withColumn("year", functions.lit(2020))
        crimepred1 = crimepred.select(crimepred["lat"],crimepred["long"],crimepred["year"],crimepred["code"])

        # load the model
        model = PipelineModel.load('predictor_model')

        # use the model to make predictions
        predict = model.transform(crimepred1)
        predict.registerTempTable("predict")
        crimesp.registerTempTable("types")
        predictions = spark.sql("select p.year as Year,t.type as Type, CAST(ROUND(AVG(p.prediction),0) as int) as Predictions from predict p join types t where p.code = t.code group by Year,Type")

        #converting to pandas
        result_pdf = predictions.select("*").toPandas()
        statement = 'The table shown above is the estimated crimes that can happen for the selected neighbourhood ' + neighbourhood + ' for mentioned year with in a radius of 1.5km'
        return render_template('home.html', tables=[result_pdf.to_html(classes='data')], header="true", values=statement)
    else:
        invalidcity = 'Our prediction only works or 7 cities mentioned in the Canada'
        return render_template('home.html',values=invalidcity)

if __name__ == '__main__':
    app.run(debug=True)