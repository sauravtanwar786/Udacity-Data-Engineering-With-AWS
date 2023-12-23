import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1702651023934 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1702651023934",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1702651047200 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1702651047200",
)

# Script generated for node Join
Join_node1702651066860 = Join.apply(
    frame1=AWSGlueDataCatalog_node1702651023934,
    frame2=AWSGlueDataCatalog_node1702651047200,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1702651066860",
)

# Script generated for node Drop Fields
DropFields_node1702651273355 = DropFields.apply(
    frame=Join_node1702651066860,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "phone",
        "lastUpdateDate",
        "email",
    ],
    transformation_ctx="DropFields_node1702651273355",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1702652719499 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1702651273355,
    database="stedi",
    table_name="accelerometer_trusted",
    additional_options={"enableUpdateCatalog": True, "updateBehavior": "LOG"},
    transformation_ctx="AWSGlueDataCatalog_node1702652719499",
)

job.commit()
