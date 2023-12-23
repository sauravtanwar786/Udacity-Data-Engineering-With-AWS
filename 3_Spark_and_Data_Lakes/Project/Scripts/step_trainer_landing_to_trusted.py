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

customer_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated"
)

step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing"
)
data_frame1 = customer_curated.toDF()
data_frame2 = step_trainer_landing.toDF()

data_frame1.createOrReplaceTempView("customer_curated_view")
data_frame2.createOrReplaceTempView("step_trainer_landing_view")

df = spark.sql('''
select  stl.sensorReadingTime,stl.serialNumber,stl.distanceFromObject

from customer_curated_view cc
join step_trainer_landing_view stl
on
    cc.serialnumber = stl.serialNumber
''')


# df.write.format("json").option('path', "s3://stlake/step_trainer/trusted/").saveAsTable("stedi.step_trainer_trusted", mode='overwrite')

final_df = DynamicFrame.fromDF(df, glueContext, "final_df")

glueContext.write_dynamic_frame.from_options(\
frame = final_df,\
connection_options = {'path': 's3://stlake/step_trainer/trusted/', 'database': 'stedi', 'table': 'step_trainer_trusted'},\
connection_type = 's3',\
format = 'json')

job.commit()