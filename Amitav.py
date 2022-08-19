import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import logging

"""
This script will use soundex function from Spark SQL library and try to match the FULLNAME columns
"""

class SparkJob:

    # initialize a pyspark job
    def __init__(self, appname: str):
        self.spark = SparkSession. \
            builder \
            .appName(appname) \
            .config('spark.sql.parquet.enableVectorizedReader', "false") \
            .getOrCreate()

    # function to read parquet files
    def read_parquet(self, paths):
        df = self.spark.read.parquet(paths)
        return df


spark_job = SparkJob('FuzzyMatch')


# read source files (Replace the file names)
logging.info("reading source files")
pr_df = spark_job.read_parquet("s3://amitav-airflow-test/testfiles/PRData_NY.parquet")
sm_df = spark_job.read_parquet("s3://amitav-airflow-test/testfiles/SMdata_NY.parquet")

#cache the data for better performance (Not required)
pr_df.cache()
sm_df.cache()

# add new columns to dataframes by using soundex function
pr_df = pr_df.withColumn("soundex_col", f.soundex(f.col('FULLNAME')))
sm_df = sm_df.withColumn("soundex_col", f.soundex(f.col('FULLNAME')))

# join the dataframes
s_DF = pr_df.join(
  sm_df,
  pr_df["soundex_col"] == sm_df["soundex_col"], 'inner')\
    .select(sm_df['*'],
            pr_df['PROVIDERID'], pr_df['FULLNAME'].alias('PR_FULLNAME'),
            pr_df['soundex_col'].alias('PR_soundex'))

# write top 10000 records into parquet. remove limit if you want to write the whole dataframe
s_DF.limit(10000).write.parquet(path="s3://amitav-airflow-test/testfiles/output/limitedrecords",compression='snappy', mode="overwrite")

print("file processed")