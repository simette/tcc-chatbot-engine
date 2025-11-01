# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient()
vector_index_name = "vector-search-inss-chatbot"
source_table = "tcc_workspace_catalog.inss_crawled_data.direitos_aposentadoria"
index_name = "tcc_workspace_catalog.inss_crawled_data.direitos_aposentadoria_index"

client.create_endpoint(
    name=vector_index_name,
    endpoint_type="STANDARD"
)

index = client.create_delta_sync_index(
  endpoint_name=vector_index_name,
  source_table_name=source_table,
  index_name=index_name,
  pipeline_type="TRIGGERED",
  primary_key="retirement_type",
  embedding_source_column="html_page_content",
  embedding_model_endpoint_name="databricks-gte-large-en"
)
