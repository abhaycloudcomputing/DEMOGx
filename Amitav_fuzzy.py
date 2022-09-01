
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import logging
from fuzzywuzzy import process

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



def apply_fuzzy_logic(x, s, threshold):
    ret_val = ''
    out = process.extract(x, s.value, limit=1)

# get the threshold numbers
    for items in out:
        print(items)
        if items[1] > threshold:
            ret_val = items[0]

    return ret_val



def fuzzy_merge(df_1, df_2, key1, threshold=90, limit=1):
    """
    :param df_1: the left table to join
    :param df_2: the right table to join
    :param key1: key column of the left table
    :param key2: key column of the right table
    :param threshold: how close the matches should be to return a match, based on Levenshtein distance
    :param limit: the amount of matches that will get returned, these are sorted high to low
    :return: dataframe with boths keys and matches
    """


    apply_logic_udf = f.udf(lambda x: apply_fuzzy_logic(x, df_2, threshold), StringType())
    df_1 = df_1.withColumn('matches', apply_logic_udf(f.col(key1)))
    return df_1


def main():
    spark_job = SparkJob('FuzzyMatch')

    # read source files (Replace the file names)
    logging.info("reading source files")
    pr_df = spark_job.read_parquet(r"C:\Users\amitavm\PycharmProjects\DEMOGx\PRData_NY.parquet")
    sm_df = spark_job.read_parquet(r"C:\Users\amitavm\PycharmProjects\DEMOGx\SMdata_NY.parquet")

    sm_df = sm_df.limit(10)

    s = pr_df.select('FULLNAME')
    s = s.rdd.flatMap(lambda x: x).collect()

    broadcast_s = spark_job.spark.sparkContext.broadcast(s)
    s_DF = fuzzy_merge(sm_df, broadcast_s, 'FULLNAME')

    # write records into parquet. remove limit if you want to write the whole dataframe
    s_DF.write.parquet(path=r"C:\Users\amitavm\PycharmProjects\DEMOGx\output", compression='snappy', mode="overwrite")

    print("file processed")


if __name__ == '__main__':
    main()
