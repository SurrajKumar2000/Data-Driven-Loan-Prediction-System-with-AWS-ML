from pyspark.sql import SparkSession
import sys

def get_brankprospect_age_greate_than_thirty(data_source, output_uri):
    with SparkSession.builder.appName("Bank data analysis").getOrCreate() as spark:

        # Load the movie rating csv data
        if data_source is not None:
            bank_data_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        bank_data_df.createOrReplaceTempView("bank_prospect_dataset")

        analysis = spark.sql(
            """SELECT * FROM bank_prospect_dataset
            WHERE age > 30 and country='USA'
            ORDER BY age""")

        analysis.write.option("header","true").mode("overwrite").csv(output_uri)

        # Stop the underlying SparkContext
        spark.stop


if __name__ == "__main__":
    get_brankprospect_age_greate_than_thirty(sys.argv[1], sys.argv[2])