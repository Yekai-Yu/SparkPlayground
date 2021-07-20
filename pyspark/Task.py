from pyspark.sql import *
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from decimal import *

import boto3
import json
import time

spark = SparkSession \
    .builder \
    .appName("TestKinesis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def stop_stream_query(query, wait_time):
    while query.isActive:
        msg = query.status['message']
        data_avail = query.status['isTriggerActive']
        trigger_active = query.status['isTriggerActive']
        if not data_avail and not trigger_active and msg!='Initializing sources':
            print('Stopping query')
            query.stop()
        time.sleep(0.5)
    print("Awaiting Termination ...")
    query.awaitTermination(wait_time)

schema = StructType() \
    .add("DayOfWeek", StringType(), True) \
    .add("FlightDate", StringType(), True) \
    .add("UniqueCarrier", StringType(), True) \
    .add("Origin", StringType(), True) \
    .add("Dest", StringType(), True) \
    .add("CRSDepTime", StringType(), True) \
    .add("DepTime", StringType(), True) \
    .add("DepDelay", StringType(), True) \
    .add("CRSArrTime", StringType(), True) \
    .add("ArrTime", StringType(), True) \
    .add("ArrDelay", StringType(), True) \
    .add("FlightNum", StringType(), True)

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "b-1.mp2.zetw14.c11.kafka.us-east-1.amazonaws.com:9092,b-2.mp2.zetw14.c11.kafka.us-east-1.amazonaws.com:9092") \
  .option("subscribe", "AWSKafkaTutorialTopic") \
  .option("startingOffsets", "earliest") \
  .load()

df = df.selectExpr("CAST(value AS STRING)") \
       .select(from_json(col('value'), schema).alias("data")).select("data.*") \

'''
# =============== Q1.2 ===============
q1dot2df = df.select(col("DayOfWeek"), col("ArrDelay")) \
             .groupBy(col("DayOfWeek")) \
             .agg({"ArrDelay": "avg"}) \
             .orderBy("AVG(ArrDelay)")

# =============== Q1.3 ===============
q1dot3df = df.select(col("UniqueCarrier"), col("ArrDelay")) \
             .groupBy(col("UniqueCarrier")) \
             .agg({"ArrDelay": "avg"}) \
             .orderBy("AVG(ArrDelay)") \
             .limit(11)

'''
# =============== Q2.1 ===============
q2dot1df_SRQ = df.select("Origin", "UniqueCarrier", "DepDelay") \
                .where(col("Origin") == "SRQ") \
                .groupBy(col("Origin"), col("UniqueCarrier")) \
                .agg({"DepDelay": "avg"}) \
                .orderBy(col("AVG(DepDelay)").asc()) \
                .limit(10)

q2dot1df_CMH = df.select("Origin", "UniqueCarrier", "DepDelay") \
                .where(col("Origin") == "CMH") \
                .groupBy(col("Origin"), col("UniqueCarrier")) \
                .agg({"DepDelay": "avg"}) \
                .orderBy(col("AVG(DepDelay)").asc()) \
                .limit(10)

q2dot1df_JFK = df.select("Origin", "UniqueCarrier", "DepDelay") \
                .where(col("Origin") == "JFK") \
                .groupBy(col("Origin"), col("UniqueCarrier")) \
                .agg({"DepDelay": "avg"}) \
                .orderBy(col("AVG(DepDelay)").asc()) \
                .limit(10)

q2dot1df_SEA = df.select("Origin", "UniqueCarrier", "DepDelay") \
                .where(col("Origin") == "SEA") \
                .groupBy(col("Origin"), col("UniqueCarrier")) \
                .agg({"DepDelay": "avg"}) \
                .orderBy(col("AVG(DepDelay)").asc()) \
                .limit(10)

q2dot1df_BOS = df.select("Origin", "UniqueCarrier", "DepDelay") \
                .where(col("Origin") == "BOS") \
                .groupBy(col("Origin"), col("UniqueCarrier")) \
                .agg({"DepDelay": "avg"}) \
                .orderBy(col("AVG(DepDelay)").asc()) \
                .limit(10)


# =============== Q2.2 ===============
q2dot2df_SRQ = df.select("Origin", "Dest", "DepDelay") \
                .where(col("Origin") == "SRQ") \
                .groupBy(col("Origin"), col("Dest")) \
                .agg({"DepDelay": "avg"}) \
                .orderBy(col("AVG(DepDelay)").asc()) \
                .limit(10)

q2dot2df_CMH = df.select("Origin", "Dest", "DepDelay") \
                .where(col("Origin") == "CMH") \
                .groupBy(col("Origin"), col("Dest")) \
                .agg({"DepDelay": "avg"}) \
                .orderBy(col("AVG(DepDelay)").asc()) \
                .limit(10)

q2dot2df_JFK = df.select("Origin", "Dest", "DepDelay") \
                .where(col("Origin") == "JFK") \
                .groupBy(col("Origin"), col("Dest")) \
                .agg({"DepDelay": "avg"}) \
                .orderBy(col("AVG(DepDelay)").asc()) \
                .limit(10)

q2dot2df_SEA = df.select("Origin", "Dest", "DepDelay") \
                .where(col("Origin") == "SEA") \
                .groupBy(col("Origin"), col("Dest")) \
                .agg({"DepDelay": "avg"}) \
                .orderBy(col("AVG(DepDelay)").asc()) \
                .limit(10)

q2dot2df_BOS = df.select("Origin", "Dest", "DepDelay") \
                .where(col("Origin") == "BOS") \
                .groupBy(col("Origin"), col("Dest")) \
                .agg({"DepDelay": "avg"}) \
                .orderBy(col("AVG(DepDelay)").asc()) \
                .limit(10)

# =============== Q2.3 ===============
q2dot3df_1 = df.select("Origin", "Dest", "UniqueCarrier", "ArrDelay") \
                .where((col("Origin") == "LGA") & (col("Dest") == "BOS")) \
                .groupby("Origin", "Dest", "UniqueCarrier") \
                .agg({"ArrDelay": "avg"}) \
                .orderBy(col("AVG(ArrDelay)").asc()) \
                .limit(10)

q2dot3df_2 = df.select("Origin", "Dest", "UniqueCarrier", "ArrDelay") \
                .where((col("Origin") == "BOS") & (col("Dest") == "LGA")) \
                .groupby("Origin", "Dest", "UniqueCarrier") \
                .agg({"ArrDelay": "avg"}) \
                .orderBy(col("AVG(ArrDelay)").asc()) \
                .limit(10)

q2dot3df_3 = df.select("Origin", "Dest", "UniqueCarrier", "ArrDelay") \
                .where((col("Origin") == "OKC") & (col("Dest") == "DFW")) \
                .groupby("Origin", "Dest", "UniqueCarrier") \
                .agg({"ArrDelay": "avg"}) \
                .orderBy(col("AVG(ArrDelay)").asc()) \
                .limit(10)

q2dot3df_4 = df.select("Origin", "Dest", "UniqueCarrier", "ArrDelay") \
                .where((col("Origin") == "MSP") & (col("Dest") == "ATL")) \
                .groupby("Origin", "Dest", "UniqueCarrier") \
                .agg({"ArrDelay": "avg"}) \
                .orderBy(col("AVG(ArrDelay)").asc()) \
                .limit(10)
'''
# =============== Q3.2 ===============
df_2008 = df
df_2008 = df_2008.withColumn("FlightDateUniform", 
                            to_date(concat(lpad(col("Month"),2,'0'), 
                                           lpad(col("DayofMonth"),2,'0'), 
                                           col("Year")), 
                                    "MMddyyyy"))
df_2008 = df_2008.filter(df_2008["FlightDateUniform"].between("2008-01-01","2008-12-31"))
df_2008 = df_2008.withColumn("CRSDepTimeUniform", 
                            to_timestamp(concat(col("FlightDateUniform"), 
                                                lit(" "), 
                                                lpad(col("CRSDepTime"),4,'0')), 
                                        "yyyy-MM-dd HHmm"))

print("first leg, before 12:00PM")
df_X_Y = df_2008.filter(concat(lpad(hour(col("CRSDepTimeUniform")),2,'0'), 
                               lpad(minute(col("CRSDepTimeUniform")), 2, '0'))
                        .between(lit("0000"), lit("1200")))
df_X_Y = df_X_Y.alias("XY")
# individual date arrival performance
df_X_Y_groupby = df_X_Y.groupBy("Origin", "Dest", "FlightDateUniform") \
                       .agg({"ArrDelay": "min"})
df_X_Y_groupby = df_X_Y_groupby.alias("XYGroupBy")
X_Y_cond = [(col("XY.Origin") == col("XYGroupBy.Origin")), 
            (col("XY.Dest") == col("XYGroupBy.Dest")), 
            (col("XY.FlightDateUniform") == col("XYGroupBy.FlightDateUniform")), 
            (col("XY.ArrDelay") == col("XYGroupBy.min(ArrDelay)"))]
df_X_Y = df_X_Y.join(df_X_Y_groupby, X_Y_cond, "inner")
df_X_Y = df_X_Y.drop("min(ArrDelay)")

print("second leg, after 12:00PM")
df_Y_Z = df_2008.filter(concat(lpad(hour(col("CRSDepTimeUniform")),2,'0'), 
                               lpad(minute(col("CRSDepTimeUniform")), 2, '0'))
                        .between(lit("1200"), lit("2400")))
df_Y_Z = df_Y_Z.alias("YZ")
# individual date arrival performance
df_Y_Z_groupby = df_Y_Z.groupBy("Origin", "Dest", "FlightDateUniform").agg({"ArrDelay": "min"})
df_Y_Z_groupby = df_Y_Z_groupby.alias("YZGroupBy")
Y_Z_cond = [(col("YZ.Origin") == col("YZGroupBy.Origin")), 
            (col("YZ.Dest") == col("YZGroupBy.Dest")), 
            (col("YZ.FlightDateUniform") == col("YZGroupBy.FlightDateUniform")), 
            (col("YZ.ArrDelay") == col("YZGroupBy.min(ArrDelay)"))]
df_Y_Z = df_Y_Z.join(df_Y_Z_groupby, Y_Z_cond, "inner")
df_Y_Z = df_Y_Z.drop("min(ArrDelay)")

print("*** Joining XY & YZ ***")
df_X_Y_Z = df_X_Y.join(df_Y_Z, col("XY.Dest") == col("YZ.Origin"), 'inner') \
                 .where(datediff(col("YZ.FlightDateUniform"), 
                                 col("XY.FlightDateUniform")) == 2)

print("*** Outputing ***")
df_X_Y_Z_select = df_X_Y_Z.select(col("XY.Origin").alias("Origin 1"), 
                                  col("XY.Dest").alias("Destination 1"), 
                                  concat(col("XY.UniqueCarrier"), 
                                         lit(" "), 
                                         col("XY.FlightNum"))
                                         .alias("Airline/Flight Number 1"), 
                                  date_format(col("XY.CRSDepTimeUniform"), "HH:mm dd/MM/yyyy")
                                            .alias("Sched Depart 1"), 
                                  col("XY.ArrDelay").alias("Arrival Delay 1"), 
                                  col("YZ.Origin").alias("Origin 2"), 
                                  col("YZ.Dest").alias("Destination 2"), 
                                  concat(col("YZ.UniqueCarrier"), 
                                         lit(" "), 
                                         col("YZ.FlightNum"))
                                         .alias("Airline/Flight Number 2"), 
                                  date_format(col("YZ.CRSDepTimeUniform"), "HH:mm dd/MM/yyyy")
                                            .alias("Sched Depart 2"), 
                                  col("YZ.ArrDelay").alias("Arrival Delay 2"))

q3dot2_df1 = df_X_Y_Z_select.where(col("XY.Origin") == "BOS" & col("XY.Dest") == "ATL" & col("YZ.Dest") == "LAX" & col("Sched Depart 1").like("%2008-04-03%"))
q3dot2_df2 = df_X_Y_Z_select.where(col("XY.Origin") == "PHX" & col("XY.Dest") == "JFK" & col("YZ.Dest") == "MSP" & col("Sched Depart 1").like("%2008-09-07%"))
q3dot2_df3 = df_X_Y_Z_select.where(col("XY.Origin") == "DFW" & col("XY.Dest") == "STL" & col("YZ.Dest") == "ORD" & col("Sched Depart 1").like("%2008-01-24%"))
q3dot2_df4 = df_X_Y_Z_select.where(col("XY.Origin") == "LAX" & col("XY.Dest") == "MIA" & col("YZ.Dest") == "LAX" & col("Sched Depart 1").like("%2008-05-16%"))

'''

q2_1_table_name = "q2.1.0"
q2_2_table_name = "q2.2.0"
q2_3_table_name = "q2.3.0"

def get_dynamodb():
  return boto3.resource('dynamodb', region_name = "us-east-1")

class Q2_1_SendToDynamoDB_ForeachWriter:
  '''
  Class to send a set of rows to DynamoDB.
  When used with `foreach`, copies of this class is going to be used to write
  multiple rows in the executor. See the python docs for `DataStreamWriter.foreach`
  for more details.
  '''

  def open(self, partition_id, epoch_id):
    # This is called first when preparing to send multiple rows.
    # Put all the initialization code inside open() so that a fresh
    # copy of this class is initialized in the executor where open()
    # will be called.
    self.dynamodb = get_dynamodb()
    return True

  def process(self, row):
    # This is called for each row after open() has been called.
    # This implementation sends one row at a time.
    # For further enhancements, contact the Spark+DynamoDB connector
    # team: https://github.com/audienceproject/spark-dynamodb
    row
    self.dynamodb.Table(q2_1_table_name).put_item(
        Item = { 'origin-carrier': str(row['Origin']) + "-" + str(row['UniqueCarrier']), 
                 'avgDelay': str(row['avg(DepDelay)']) })

  def close(self, err):
    # This is called after all the rows have been processed.
    if err:
      raise err

class Q2_2_SendToDynamoDB_ForeachWriter:
  '''
  Class to send a set of rows to DynamoDB.
  When used with `foreach`, copies of this class is going to be used to write
  multiple rows in the executor. See the python docs for `DataStreamWriter.foreach`
  for more details.
  '''

  def open(self, partition_id, epoch_id):
    # This is called first when preparing to send multiple rows.
    # Put all the initialization code inside open() so that a fresh
    # copy of this class is initialized in the executor where open()
    # will be called.
    self.dynamodb = get_dynamodb()
    return True

  def process(self, row):
    # This is called for each row after open() has been called.
    # This implementation sends one row at a time.
    # For further enhancements, contact the Spark+DynamoDB connector
    # team: https://github.com/audienceproject/spark-dynamodb
    self.dynamodb.Table(q2_2_table_name).put_item(
        Item = { 'origin-dest': str(row['Origin']) + "-" + str(row['Dest']), 
                 'avgDelay': str(row['avg(DepDelay)']) })

  def close(self, err):
    # This is called after all the rows have been processed.
    if err:
      raise err

class Q2_3_SendToDynamoDB_ForeachWriter:
  '''
  Class to send a set of rows to DynamoDB.
  When used with `foreach`, copies of this class is going to be used to write
  multiple rows in the executor. See the python docs for `DataStreamWriter.foreach`
  for more details.
  '''

  def open(self, partition_id, epoch_id):
    # This is called first when preparing to send multiple rows.
    # Put all the initialization code inside open() so that a fresh
    # copy of this class is initialized in the executor where open()
    # will be called.
    self.dynamodb = get_dynamodb()
    return True

  def process(self, row):
    # This is called for each row after open() has been called.
    # This implementation sends one row at a time.
    # For further enhancements, contact the Spark+DynamoDB connector
    # team: https://github.com/audienceproject/spark-dynamodb
    self.dynamodb.Table(q2_3_table_name).put_item(
        Item = { 'origin-dest-carrier': str(row['Origin']) + "-" + str(row['UniqueCarrier']) + "-" + str(row['UniqueCarrier']), 
                 'avgDelay': str(row['avg(ArrDelay)']) })

  def close(self, err):
    # This is called after all the rows have been processed.
    if err:
      raise err


def execute(df_, t):
    q = (
        df_.writeStream\
            .outputMode("complete") \
            .option("truncate", "false")\
            .format("console") \
            .start()
    )
    stop_stream_query(q, t)

def execute_q2(df_, t, writer):
    q = (
        df_.writeStream\
            .foreach(writer) \
            .outputMode("complete") \
            .start()
    )
    stop_stream_query(q, t)
    print("--- Done ---")


# =============== Execution ===============
interval = 5

#execute(q1dot2df, interval)
# execute(q1dot3df, interval)

execute_q2(q2dot1df_SRQ, interval, Q2_1_SendToDynamoDB_ForeachWriter())
execute_q2(q2dot1df_CMH, interval, Q2_1_SendToDynamoDB_ForeachWriter())
execute_q2(q2dot1df_JFK, interval, Q2_1_SendToDynamoDB_ForeachWriter())
execute_q2(q2dot1df_SEA, interval, Q2_1_SendToDynamoDB_ForeachWriter())
execute_q2(q2dot1df_BOS, interval, Q2_1_SendToDynamoDB_ForeachWriter())

execute_q2(q2dot2df_SRQ, interval, Q2_2_SendToDynamoDB_ForeachWriter())
execute_q2(q2dot2df_CMH, interval, Q2_2_SendToDynamoDB_ForeachWriter())
execute_q2(q2dot2df_JFK, interval, Q2_2_SendToDynamoDB_ForeachWriter())
execute_q2(q2dot2df_SEA, interval, Q2_2_SendToDynamoDB_ForeachWriter())
execute_q2(q2dot2df_BOS, interval, Q2_2_SendToDynamoDB_ForeachWriter())

execute_q2(q2dot3df_1, interval, Q2_3_SendToDynamoDB_ForeachWriter())
execute_q2(q2dot3df_2, interval, Q2_3_SendToDynamoDB_ForeachWriter())
execute_q2(q2dot3df_3, interval, Q2_3_SendToDynamoDB_ForeachWriter())
execute_q2(q2dot3df_4, interval, Q2_3_SendToDynamoDB_ForeachWriter())
'''
execute(q3dot2_df1, interval)
execute(q3dot2_df2, interval)
execute(q3dot2_df3, interval)
execute(q3dot2_df4, interval)
'''