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

# Script generated for node Trainer Landing
TrainerLanding_node1792063435816 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://daras-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="TrainerLanding_node1792063435816")

# Script generated for node Customer Curated 
CustomerCurated_node1792063826219 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1792063826219")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1792065062495 = ApplyMapping.apply(frame=TrainerLanding_node1792063435816, mappings=[("sensorreadingtime", "bigint", "sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1792065062495")

# Script generated for node Join
Join_node1792063512993 = Join.apply(frame1=CustomerCurated_node1792063826219, frame2=RenamedkeysforJoin_node1792065062495, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1792063512993")

# Script generated for node Drop Fields 
SqlQuery532 = '''
select distinct serialnumber, sensorreadingtime, right_distancefromobject as distancefromobject
 from myDataSource
'''
DropFields_node1792064112607 = sparkSqlQuery(glueContext, query = SqlQuery532, mapping = {"myDataSource":Join_node1792063512993}, transformation_ctx = "DropFields_node1792064112607")

# Script generated for node Trainer Trusted 
EvaluateDataQuality().process_rows(frame=DropFields_node1792064112607, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792057682274", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
TrainerTrusted_node1792064624565 = glueContext.getSink(path="s3://daras-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrainerTrusted_node1792064624565")
TrainerTrusted_node1792064624565.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
TrainerTrusted_node1792064624565.setFormat("json")
TrainerTrusted_node1792064624565.writeFrame(DropFields_node1792064112607)
job.commit()