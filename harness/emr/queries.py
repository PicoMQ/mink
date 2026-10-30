import sys

from pyspark.sql import SparkSession


def main():
    table, paths = sys.argv[1], sys.argv[2:]
    spark = SparkSession.builder.getOrCreate()

    for path in paths:
        name = path.rsplit("/", 1)[-1].removesuffix(".sql").upper()
        query = spark.sparkContext.wholeTextFiles(path).first()[1]
        rows = spark.sql(query % table).collect()

        print(name)
        for row in rows:
            print("  " + repr(list(row)))


main()
