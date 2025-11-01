from delta.tables import DeltaTable
from pyspark.sql.functions import col

df = spark.readStream.option("readChangeFeed", "true").table("tcc_workspace_catalog.raw_inss_crawled_data.direitos_aposentadoria")

def upsert_to_delta(microBatchOutputDF, batchId):
    """
    This function performs an upsert operation on the Delta table.
    It takes a micro-batch DataFrame and merges it into the target Delta table.
    """
    # Get the DeltaTable object
    deltaTable = DeltaTable.forName(spark, "tcc_workspace_catalog.inss_crawled_data.direitos_aposentadoria")
    #deltaTable = DeltaTable.forPath(spark, delta_table_path)

    deltaTable.alias("target") \
        .merge(
            microBatchOutputDF.alias("source"),
            "target.retirement_type = source.retirement_type"
        ) \
        .whenMatchedUpdate(set = {
            "html_page_content": col("source.html_page_content"),
            "scraped_at": col("source.scraped_at"),
            "last_update_on_inss_site": col("source.last_update_on_inss_site"),
        }) \
        .whenNotMatchedInsertAll() \
        .execute()

query = df.writeStream \
    .foreachBatch(upsert_to_delta) \
    .outputMode("update") \
    .option("checkpointLocation", "/Volumes/tcc_workspace_catalog/raw_inss_crawled_data/checkpoint_volume/direitos_aposentadoria") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()
