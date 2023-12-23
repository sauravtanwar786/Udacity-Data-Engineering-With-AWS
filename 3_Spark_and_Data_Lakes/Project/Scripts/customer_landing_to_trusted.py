import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1702635410798 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1702635410798",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from customer_landing where shareWithResearchAsOfDate is not null
"""
SQLQuery_node1702638208581 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_landing": AWSGlueDataCatalog_node1702635410798},
    transformation_ctx="SQLQuery_node1702638208581",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1702639316414 = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node1702638208581,
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1702639316414",
)

job.commit()