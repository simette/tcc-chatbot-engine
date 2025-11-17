from operator import itemgetter
import mlflow, os
import constants

from databricks.vector_search.client import VectorSearchClient

from langchain_community.chat_models import ChatDatabricks
from langchain_community.vectorstores import DatabricksVectorSearch

from langchain_core.runnables import RunnableLambda, RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from mlflow.entities import SpanType, Document

def initialize_mlflow_logging():
    mlflow.langchain.autolog()
    mlflow.set_tracking_uri('databricks')
    mlflow.set_experiment('/Shared/logs')


@mlflow.trace
def extract_user_query_string(input_data):
    return input_data[-1]["content"]


@mlflow.trace
def extract_chat_history(input_data):
    return str(input_data[:-1])

def generate_vector_search_question(input_data):
    if len(input_data) >= 3:        
        current_question = input_data[-1]["content"]
        last_chat_answer = input_data[-2]["content"]
        last_question = input_data[-3]["content"]
        return f"LAST QUESTION: { last_question }\nLAST ANSWER: { last_chat_answer }\nCURRENT QUESTION: { current_question }"
    
    return input_data[-1]["content"]

@mlflow.trace
def build_rag_chain():
    model = get_foundation_model(model_name=constants.LLM_MODEL)
    
    retriever = get_vector_index(
        endpoint_name=constants.VECTOR_SEARCH_ENDPOINT,
        index_name=constants.VECTOR_INDEX_DATABASE,
    )
    
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", constants.PROMPT_TEMPLATE),
            ("user", "{question}"),
        ]
    )
    chain = (
        {
            "question": itemgetter("messages") | RunnableLambda(extract_user_query_string),
            "chat_history": itemgetter("messages") | RunnableLambda(extract_chat_history),
            "vector_search_question": itemgetter("messages") | RunnableLambda(generate_vector_search_question)
        }
        | RunnablePassthrough()
        |
        {
            "context": itemgetter("vector_search_question") | retriever | RunnableLambda(format_context),
            "chat_history": itemgetter("chat_history"),
            "question": itemgetter("question"),
        }
        | prompt
        | model
        | StrOutputParser()
    )
    
    return chain


@mlflow.trace
def get_foundation_model(model_name: str):
    """Initialize and return the foundation model"""
    print(f"Reading model {model_name}")
    return ChatDatabricks(
        endpoint=model_name,
        extra_params={"temperature": 0.00},
    )


@mlflow.trace(span_type=SpanType.RETRIEVER)
def get_vector_index(endpoint_name: str, index_name: str, k_results=1):
    vs_client = VectorSearchClient(disable_notice=True)
    vs_index = vs_client.get_index(
        endpoint_name=endpoint_name,
        index_name=index_name,
    )

    vector_search_as_retriever = DatabricksVectorSearch(
        vs_index,
        text_column="html_page_content",
        columns=[
            "retirement_type",
            "page_url",
            "html_page_content",
        ],
    ).as_retriever(search_kwargs={"k": k_results})

    return vector_search_as_retriever


@mlflow.trace
def format_context(docs):
    chunk_template = "Passage: {chunk_text}\n"
    chunk_contents = [
        chunk_template.format(
            chunk_text=d.page_content,
        )
        for d in docs
    ]
    return "".join(chunk_contents)


initialize_mlflow_logging()

chain = build_rag_chain()
mlflow.models.set_model(model=chain)
