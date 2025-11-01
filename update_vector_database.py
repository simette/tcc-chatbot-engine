# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vector_index_name = "tcc_workspace_catalog.inss_crawled_data.direitos_aposentadoria_index"
vsc = VectorSearchClient()

index = vsc.get_index(
    index_name=vector_index_name
)
index.sync()
