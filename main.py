from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from langchain_core.messages import HumanMessage
from pydantic import BaseModel
from typing import Optional
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import START, StateGraph
from langchain_ollama import ChatOllama
from agents.csv_to_graph import CsvToGraphAgent, State
import uuid



app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",  # your Java frontend
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#region agent setup. TODO: productionize ts

llm = ChatOllama(model="qwen3:4b-instruct-2507-q4_K_M", temperature=0.1)

# Initialize checkpointer for conversation memory
checkpointer = InMemorySaver()

# Create graph builder
graph_builder = StateGraph(State)

agent = CsvToGraphAgent(llm)

# Add nodes
graph_builder.add_node("parse_user_question", agent.parse_user_question)
graph_builder.add_node("get_unique_nouns", agent.get_unique_nouns)
graph_builder.add_node("get_sql_query", agent.get_sql_query)
graph_builder.add_node("validate_and_fix_sql", agent.validate_and_fix_sql)
graph_builder.add_node("execute_sql", agent.execute_sql)
graph_builder.add_node("choose_visualization", agent.choose_visualization)
graph_builder.add_node("visualize", agent.visualize)
graph_builder.add_node("answer_with_visual", agent.answer_with_visual)
graph_builder.add_node("answer_with_data", agent.answer_with_data)
##graph_builder.add_node("vega_lite_visualizer", vega_lite_visualizer)

graph_builder.add_edge(START, "parse_user_question")
graph_builder.add_conditional_edges("parse_user_question", agent.route_after_question_parse)
graph_builder.add_edge("get_unique_nouns", "get_sql_query")
graph_builder.add_edge("get_sql_query", "validate_and_fix_sql")
graph_builder.add_edge("validate_and_fix_sql", "execute_sql")
graph_builder.add_edge("execute_sql", "choose_visualization")
graph_builder.add_conditional_edges("choose_visualization", agent.route_after_choose_visualization)
graph_builder.add_edge("visualize", "answer_with_visual")

# Compile graph with checkpointer
agent_graph = graph_builder.compile(
    checkpointer=checkpointer,
)

# endregion

class ChatRequest(BaseModel):
    message: str
    table_name : str
    thread_id: Optional[str] = None

class ChatResponse(BaseModel):
    reply: str
    thread_id: str
    image_base64: Optional[str] = None

def create_thread_id():
    return {"configurable": {"thread_id": str(uuid.uuid4())}}

@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):

    config = (
        {"configurable": {"thread_id": req.thread_id}}
        if req.thread_id
        else create_thread_id()
    )

    result = agent_graph.invoke(
        {"messages": [HumanMessage(content=req.message.strip())],
          "table_name": req.table_name},
        config
    )

    image = result.get("generated_chart", None)

    image_base64 = None

    #TODO: figure out the images
    if image:
        import base64
        image_base64 = base64.b64encode(image.getvalue()).decode()

    return ChatResponse(
        reply=result["messages"][-1].content,
        thread_id=config["configurable"]["thread_id"],
        image_base64=image_base64
    )