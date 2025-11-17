# Databricks notebook source
!pip install -U "mlflow>=3" "langchain==0.3.7" "langchain-core==0.3.15" "langchain-community==0.3.5" "databricks-vectorsearch>=0.40" "databricks-agents>=0.19.0" "pydantic>=2.9.0,<3.0.0" "cloudpickle>=2.2.1"


# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow, os, constants
from mlflow.models.resources import (
  DatabricksVectorSearchIndex,
  DatabricksServingEndpoint,
)

# COMMAND ----------

chain_config = {
    "llm_model_serving_endpoint_name": constants.LLM_MODEL,
    "vector_search_endpoint_name": constants.VECTOR_SEARCH_ENDPOINT,
    "vector_search_index": constants.VECTOR_INDEX_DATABASE,
    "llm_prompt_template":  constants.PROMPT_TEMPLATE,
}


input_example = {
    "messages": [
        {
            "role": "user", 
            "content": "quantos anos uma mulher precisa ter para se aposentar sendo professora"
        }
    ],
}

# COMMAND ----------

with mlflow.start_run(run_name="chat-model-inss-chatbot"):
    logged_chain_info = mlflow.langchain.log_model(
        lc_model=os.path.join(os.getcwd(),"rag_chain.py"),
        model_config=chain_config,
        name="langchain",
        input_example=input_example,
        pip_requirements=[
            "mlflow>=3",
            "langchain==0.3.7",
            "langchain-core==0.3.15",
            "langchain-community==0.3.5",
            "databricks-vectorsearch>=0.40",
            "databricks-agents>=0.19.0",
            "pydantic>=2.9.0,<3.0.0",
            "cloudpickle>=2.2.1",
        ],
        resources=[
            DatabricksVectorSearchIndex(index_name=constants.VECTOR_INDEX_DATABASE),
            DatabricksServingEndpoint(endpoint_name=constants.SERVING_ENDPOINT_NAME),
            DatabricksServingEndpoint(endpoint_name="databricks-meta-llama-3-3-70b-instruct"),
            DatabricksServingEndpoint(endpoint_name=constants.LLM_MODEL),
        ],
        code_paths=[
             "constants.py",
        ],

    )

# COMMAND ----------

input_example = {
    "messages": [
    {'role': 'system', 'content': ''},
    {'role': 'user', 'content': 'quais as regras para aposentadoria por tempo de contribuicao para um professor?'},
],
}
modelo=mlflow.langchain.load_model(logged_chain_info.model_uri)
modelo.invoke(input_example)
