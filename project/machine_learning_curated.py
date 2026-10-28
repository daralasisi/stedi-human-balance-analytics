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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1793205823567 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1793205823567")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1793205830964 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1793205830964")

# Script generated for node Join
Join_node1793205928547 = Join.apply(frame1=StepTrainerTrusted_node1793205823567, frame2=AccelerometerTrusted_node1793205830964, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1793205928547")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct
user,
timestamp,
x,
y,
z,
sensorreadingtime,
serialnumber,
distancefromobject
from myDataSource
'''
SQLQuery_node1793206016869 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1793205928547}, transformation_ctx = "SQLQuery_node1793206016869")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1793206016869, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1793198443088", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1793206112436 = glueContext.getSink(path="s3://daras-lake-house/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1793206112436")
AmazonS3_node1793206112436.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machin_learning_curated")
AmazonS3_node1793206112436.setFormat("json")
AmazonS3_node1793206112436.writeFrame(SQLQuery_node1793206016869)
job.commit()
