# Crime Analysis and Prediction of Major Cities in Canada

Our objective is to analyze the crime data of 7 Canadian cities over the past and predict  the  estimated  crime rates  for  the  following year

Below steps are required to make this code work:

*  Create a keyspace and the relevant tables in Cassandra DB - You would need to access the cluster and open cqlsh to perform the setups.Setup commands are present in folder 'Cassandra Keyspace and Tablesetups'

Note - Codes and Data for below steps are present in folder 'data_loaded_code'
*  Post creating the tables please run the below codes to insert values into configuration tables
        1.  Run the below code using crime_types.csv

*  Run the codes to perform ETL for the following cities . Commands are provided as below
        # To load Vancouver Data from OpenDataPortal
        1.  spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 van_load.py
        
        # To load Calgary Data from OpenDataPortal
        2.  spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 cal_load.py
        
        # To load Toronto Data from OpenDataPortal
        3.  
        
        # To load Ottawa Data from OpenDataPortal
        4.  
        
        # To load Victoria Data from OpenDataPortal
        5.  
        
        # To load Lethbridge Data from OpenDataPortal
        6.  
        
        # To load Edmonton Data from OpenDataPortal
        7.  

*  Now that our data is loaded , please run the following code to perform descriptive analytics of the loaded data
        spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 crimetypcnt.py

* We also have developed a prediction model that can predict the crime rates based on neighbourhood radius of 1.5km. To train the model, please run the below code
        spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 training_data_loader.py
        spark-submit --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 crime_predictor.py

* To render the descriptive analytics(developed in Tableau) and the prediction model, please run the below code
        spark-submit main.py

* 