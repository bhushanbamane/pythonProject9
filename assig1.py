from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number
from pyspark.sql.types import *

if __name__ == '__main__':
    spark=SparkSession.builder.master('local[*]').\
          appName('testdf').getOrCreate()

    de = spark.read.load(r"D:\TEAM BRAIN WORKS\PYSPARK\pyspark_data\assi1.csv",
                         format="csv",header=True,inferSchema=True)

    de.show()

    de.