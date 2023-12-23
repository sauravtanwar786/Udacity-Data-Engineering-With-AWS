import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
)
data_frame1 = step_trainer_trusted.toDF()
data_frame2 = accelerometer_trusted.toDF()

data_frame1.createOrReplaceTempView("step_trainer_trusted_view")
data_frame2.createOrReplaceTempView("accelerometer_trusted_view")


df = spark.sql('''
select cc.sensorReadingTime, cc.serialNumber, cc.distanceFromObject, stl.timestamp, stl.x, stl.y, stl.z
from step_trainer_trusted_view cc
join accelerometer_trusted_view stl
on
    cc.sensorReadingTime = stl.timestamp
''')


final_df = DynamicFrame.fromDF(df, glueContext, "final_df")

glueContext.write_dynamic_frame.from_options(\
frame = final_df,\
connection_options = {'path': 's3://stlake/step_trainer/curated/', 'database': 'stedi', 'table': 'machine_learning_curated'},\
connection_type = 's3',\
format = 'json')

job.commit()