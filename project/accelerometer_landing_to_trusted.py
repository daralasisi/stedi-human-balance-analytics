import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1790246932363 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1790246932363")

# Script generated for node Customer Trusted
CustomerTrusted_node1790246955069 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1790246955069")

# Script generated for node Join Customer
JoinCustomer_node1790247492790 = Join.apply(frame1=AccelerometerLanding_node1790246932363, frame2=CustomerTrusted_node1790246955069, keys1=["user"], keys2=["email"], transformation_ctx="JoinCustomer_node1790247492790")

# Script generated for node Drop Fields
SqlQuery0 = '''
select 
    user,
    timestamp,
    x,
    y,
    z
from myDataSource

'''
DropFields_node1790247860028 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":JoinCustomer_node1790247492790}, transformation_ctx = "DropFields_node1790247860028")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1790247860028, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790246831355", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1790247654532 = glueContext.getSink(path="s3://daras-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1790247654532")
AccelerometerTrusted_node1790247654532.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1790247654532.setFormat("json")
AccelerometerTrusted_node1790247654532.writeFrame(DropFields_node1790247860028)
job.commit()
