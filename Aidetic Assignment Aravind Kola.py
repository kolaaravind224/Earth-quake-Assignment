# Databricks notebook source
pip install folium #Importing python libraries 

# COMMAND ----------

#Importing all the functions required for the tasks
from pyspark.sql.functions import *
from pyspark.sql.functions import col, radians, sin, cos, sqrt, atan2
from pyspark.sql.functions import count

# COMMAND ----------

#Adding data types to the columns  
schema  = 'Date String,Time String,Latitude float,Longitude float,Type String,Depth float,Magnitude float'
#loading the data to the dataframe
Earthquake_df = spark.read.schema(schema).format("csv").option("header", "true").load("dbfs:/FileStore/Earthquake_file.csv")
Earthquake_df.show(truncate=False)

# COMMAND ----------

#checking the schema of the data frame 
Earthquake_df.printSchema()

# COMMAND ----------

# Assuming 'date_column' is the column containing date strings and 'timestamp_column' is the column containing timestamp strings
from pyspark.sql.functions import to_timestamp
Earthquake_Time_df = Earthquake_df.select(concat_ws(' ',Earthquake_df.Date,Earthquake_df.Time).alias("TimeStamp"),"Latitude","Longitude","Type","Depth","Magnitude")
Earthquake_Time_df.show()

# COMMAND ----------

#Filtering the earthquakes where magnitude is greater than 5
Earthquake_filter_df = Earthquake_Time_df.select("*").filter("Magnitude > 5").orderBy("Magnitude",ascending = False)
Earthquake_filter_df.show()

# COMMAND ----------

#Calculating the avg depth and Magnitude of each earthquake type
Earthquake_avg_df = Earthquake_filter_df.groupBy("Type").agg(round(avg("Depth"),2).alias("Depth"),round(avg("Magnitude"),2).alias("Magnitude")).orderBy("Magnitude",ascending = False)
Earthquake_avg_df.show()

# COMMAND ----------

#Categorizing the level of earthquakes to low,medium adnd High
Earthquake_cat_df = Earthquake_Time_df.withColumn("Levels_of_Earthquake",expr("CASE WHEN Magnitude <= '2.5'  then 'Low' WHEN Magnitude between '2.6' and '5.9' then'Medium' WHEN Magnitude >= '6.0' then 'High' else  Magnitude end"))
Earthquake_cat_df.show()

# COMMAND ----------

#calculating the distance of the each earthquake by taking reference as (0,0)
#creating temperory table
Earthquake_Time_df.createOrReplaceTempView("Earthquake_Table")

# COMMAND ----------

#Calculating distance using Haversine formula
#Taking reference (0,0) for ref_latitude2,ref_longitude2 points
earth_quake_dist = spark.sql("""
    SELECT 
        *,round(
        6371 * acos(
            cos(radians(Latitude1)) * cos(radians(Ref_Latitude2)) * cos(radians(Latitude1) - radians(Ref_Longitude2)) + 
            sin(radians(Latitude1)) * sin(radians(Ref_Latitude2))
        ),2) AS Distance
    FROM(select TimeStamp,Type,Depth,Magnitude,
              latitude as Latitude1,
              longitude as Longitude1,
               0 as Ref_Latitude2,
               0 as Ref_Longitude2
          
        from Earthquake_Table ) points ORDER BY Distance DESC
""")
display(earth_quake_dist)

# COMMAND ----------

#writing the distance calculating final csv to file store and downloading from the
earth_quake_dist.coalesce(1).write.csv("dbfs:/FileStore/earthquake_distance.csv", header=True)#need to check 

# COMMAND ----------

#Steps to visualizing the earthquakes using folium and other functions
# Aggregate earthquake data by latitude and longitude
Earthquake_agg_df = Earthquake_Time_df.groupBy("latitude", "longitude").agg(count("*").alias("count"))
Earthquake_agg_pandas_df = Earthquake_agg_df.toPandas()

# COMMAND ----------

#Importing folium 
import folium

# COMMAND ----------

# Creating a Folium map centered at (0, 0)
map_df = folium.Map(location=[0, 0], zoom_start=2)
# Adding markers for each earthquake location
for index, row in Earthquake_agg_pandas_df.iterrows():
    folium.CircleMarker(location=[row['latitude'], row['longitude']], radius=row['count']/100,
                        color='red', fill=True, fill_color='red').add_to(map_df)
display(map_df)
#shown below red highlighted areas are prone to earthquakes

# COMMAND ----------


