import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1702656093196 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1702656093196",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1702656115742 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1702656115742",
)

# Script generated for node Join
Join_node1702656137200 = Join.apply(
    frame1=AWSGlueDataCatalog_node1702656115742,
    frame2=AWSGlueDataCatalog_node1702656093196,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1702656137200",
)

# Script generated for node Drop Fields
DropFields_node1702656168924 = DropFields.apply(
    frame=Join_node1702656137200,
    paths=["z", "user", "y", "x", "timestamp"],
    transformation_ctx="DropFields_node1702656168924",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1702657827367 = DynamicFrame.fromDF(
    DropFields_node1702656168924.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1702657827367",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1702656195494 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropDuplicates_node1702657827367,
    database="stedi",
    table_name="customer_curated",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="AWSGlueDataCatalog_node1702656195494",
)

job.commit()
