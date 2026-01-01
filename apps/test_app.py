import os
import sys
from pathlib import Path
from openai import OpenAI

import streamlit as st
import sqlalchemy as sql
import pandas as pd
import plotly.io as pio
import json
import asyncio
from dotenv import load_dotenv

from langchain_community.chat_message_histories import StreamlitChatMessageHistory
from langchain_openai import ChatOpenAI

# Add root to path for src imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.agents.sql_agent import SQLDatabaseAgent
from src.multiagents.supervisor_ds_team import SupervisorDSTeam
from src.agents.data_wrangling_agent import DataWranglingAgent
from src.agents.data_cleaning_agent import DataCleaningAgent
from src.ds_agents.eda_tools_agent import EDAToolsAgent
from src.agents.data_visualization_agent import DataVisualizationAgent
from src.agents.feature_engineering_agent import FeatureEngineeringAgent
from src.ml_agents.h2o_ml_agent import H2OMLAgent
from src.ml_agents.model_evaluation_agent import ModelEvaluationAgent
from src.ml_agents.mlflow_tools_agent import MLflowToolsAgent
from src.agents.data_loader_tools_agent import DataLoaderToolsAgent


load_dotenv()

# * APP INPUTS ----

DB_OPTIONS = {
    "Crypto Database": f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres:5432/{os.getenv('POSTGRES_DB')}?sslmode=disable"
}

MODEL_LIST = ['gpt-4o-mini', 'gpt-4o']

TITLE = "Your SQL Database Agent"

# * STREAMLIT APP SETUP ----

st.set_page_config(page_title=TITLE, page_icon="📊")
st.title(TITLE)

st.markdown("""
Welcome to the SQL Database Agent. This AI agent is designed to help you query your SQL database and return data frames that you can interactively inspect and download.
""")

with st.expander("Example Questions", expanded=False):
    st.write(
        """
        - What tables exist in the database?
        - What are the first 10 rows in the territory table?
        - Aggregate sales for each territory. 
        - Aggregate sales by month for each territory.
        """
    )

# * STREAMLIT APP SIDEBAR ----

# Database Selection
db_option = st.sidebar.selectbox(
    "Select a Database",
    list(DB_OPTIONS.keys()),
)

st.session_state["PATH_DB"] = DB_OPTIONS.get(db_option)
sql_engine = sql.create_engine(st.session_state["PATH_DB"])
conn = sql_engine.connect()

# OpenAI API Key
st.sidebar.header("Enter your OpenAI API Key")
st.session_state["OPENAI_API_KEY"] = os.getenv("OPENAI_KEY")

if st.session_state["OPENAI_API_KEY"]:
    client = OpenAI(api_key=st.session_state["OPENAI_API_KEY"])
    try:
        models = client.models.list()
        st.success("API Key is valid!")
    except Exception as e:
        st.error(f"Invalid API Key: {e}")
else:
    st.info("Please enter your OpenAI API Key to proceed.")
    st.stop()

# OpenAI Model Selection
model_option = st.sidebar.selectbox(
    "Choose OpenAI model",
    MODEL_LIST,
    index=0
)

OPENAI_LLM = ChatOpenAI(
    model=model_option,
    api_key=st.session_state["OPENAI_API_KEY"]
)

# Agent Mode Selection
agent_mode = st.sidebar.radio(
    "Select Agent Mode",
    ["SQL Only", "Full Data Science Team"],
    help="SQL Only: Fast SQL queries. Full Team: Multi-step workflows (SQL → wrangle → visualize → model)"
)

llm = OPENAI_LLM

# * INITIALIZE AGENTS ----

# SQL Agent (always available)
sql_db_agent = SQLDatabaseAgent(
    model=llm,
    connection=conn,
    n_samples=1,
    log=False,
    bypass_recommended_steps=True,
)

# Full supervisor team (lazy-loaded only if needed)
supervisor_team = None
if agent_mode == "Full Data Science Team":
    try:
        # Initialize all sub-agents
        data_wrangling_agent = DataWranglingAgent(model=llm)
        data_cleaning_agent = DataCleaningAgent(model=llm)
        data_loader_agent = DataLoaderToolsAgent(model=llm)
        eda_tools_agent = EDAToolsAgent(model=llm)
        data_visualization_agent = DataVisualizationAgent(model=llm)
        feature_engineering_agent = FeatureEngineeringAgent(model=llm)
        h2o_ml_agent = H2OMLAgent(model=llm)
        mlflow_tools_agent = MLflowToolsAgent(model=llm)
        model_evaluation_agent = ModelEvaluationAgent(model=llm)

        # Create supervisor team
        supervisor_team = SupervisorDSTeam(
            model=llm,
            sql_database_agent=sql_db_agent,
            data_loader_agent=data_loader_agent,
            data_wrangling_agent=data_wrangling_agent,
            data_cleaning_agent=data_cleaning_agent,
            eda_tools_agent=eda_tools_agent,
            data_visualization_agent=data_visualization_agent,
            feature_engineering_agent=feature_engineering_agent,
            h2o_ml_agent=h2o_ml_agent,
            mlflow_tools_agent=mlflow_tools_agent,
            model_evaluation_agent=model_evaluation_agent,
            temperature=1.0,
        )
    except Exception as e:
        st.warning(f"Could not initialize Full Team mode: {e}. Falling back to SQL Only.")
        agent_mode = "SQL Only"

# * STREAMLIT CHAT ----

msgs = StreamlitChatMessageHistory(key="langchain_messages")
if len(msgs.messages) == 0:
    msgs.add_ai_message("How can I help you?")

if "dataframes" not in st.session_state:
    st.session_state.dataframes = []

if "plots" not in st.session_state:
    st.session_state.plots = []


def display_chat_history():
    for i, msg in enumerate(msgs.messages):
        with st.chat_message(msg.type):
            if "PLOT_INDEX:" in msg.content:
                plot_index = int(msg.content.split("PLOT_INDEX:")[1])
                st.plotly_chart(st.session_state.plots[plot_index], key=f"history_plot_{plot_index}")
            elif "DATAFRAME_INDEX:" in msg.content:
                df_index = int(msg.content.split("DATAFRAME_INDEX:")[1])
                st.dataframe(st.session_state.dataframes[df_index], key=f"history_dataframe_{df_index}")
            else:
                st.write(msg.content)


display_chat_history()

# Handle user input
async def handle_sql_only(question):
    await sql_db_agent.ainvoke_agent(user_instructions=question)
    return sql_db_agent

async def handle_full_team(question):
    await supervisor_team.ainvoke_agent(user_instructions=question)
    return supervisor_team

if st.session_state["PATH_DB"] and (question := st.chat_input("Enter your question here:", key="query_input")):
    
    if not st.session_state["OPENAI_API_KEY"]:
        st.error("Please enter your OpenAI API Key to proceed.")
        st.stop()
    
    with st.spinner("Thinking..."):
        st.chat_message("human").write(question)
        msgs.add_user_message(question)
        
        error_occured = False
        result = None
        
        try:
            if agent_mode == "SQL Only":
                result = asyncio.run(handle_sql_only(question))
            else:
                result = asyncio.run(handle_full_team(question))
        except Exception as e:
            error_occured = True
            print(e)
            response_text = f"""
            I'm sorry. I am having difficulty answering that question. You can try providing more details and I'll do my best to provide an answer.
            
            Error: {e}
            """
            msgs.add_ai_message(response_text)
            st.chat_message("ai").write(response_text)
            st.error(f"Error: {e}")
        
        # Generate Results
        if not error_occured and result:
            if agent_mode == "SQL Only":
                sql_query = result.get_sql_query_code()
                response_df = result.get_data_sql()
                
                if sql_query:
                    response_1 = f"### SQL Results:\n\nSQL Query:\n\n```sql\n{sql_query}\n```\n\nResult:"
                    df_index = len(st.session_state.dataframes)
                    st.session_state.dataframes.append(response_df)
                    msgs.add_ai_message(response_1)
                    msgs.add_ai_message(f"DATAFRAME_INDEX:{df_index}")
                    st.chat_message("ai").write(response_1)
                    st.dataframe(response_df)
            else:
                # Full team mode: check for charts or tables
                result_dict = result if isinstance(result, dict) else {}
                
                # Check for plotly chart
                plot_data = result_dict.get("plotly_graph")
                if plot_data and not result_dict.get("plotly_error", False):
                    # Convert dictionary to JSON string if needed
                    if isinstance(plot_data, dict):
                        plot_json = json.dumps(plot_data)
                    else:
                        plot_json = plot_data
                    try:
                        plot_obj = pio.from_json(plot_json)
                        response_text = "Returning the generated chart."
                        plot_index = len(st.session_state.plots)
                        st.session_state.plots.append(plot_obj)
                        msgs.add_ai_message(response_text)
                        msgs.add_ai_message(f"PLOT_INDEX:{plot_index}")
                        st.chat_message("ai").write(response_text)
                        st.plotly_chart(plot_obj)
                    except Exception as e:
                        st.warning(f"Could not render plot: {e}")
                
                # Check for data table
                data_wrangled = result_dict.get("data_wrangled")
                if data_wrangled is not None:
                    response_text = "Returning the data table."
                    if not isinstance(data_wrangled, pd.DataFrame):
                        data_wrangled = pd.DataFrame(data_wrangled)
                    df_index = len(st.session_state.dataframes)
                    st.session_state.dataframes.append(data_wrangled)
                    msgs.add_ai_message(response_text)
                    msgs.add_ai_message(f"DATAFRAME_INDEX:{df_index}")
                    st.chat_message("ai").write(response_text)
                    st.dataframe(data_wrangled)
                
                # Fallback: display last AI message
                last_ai_msg = result_dict.get("ai_message")
                if last_ai_msg and not plot_data and data_wrangled is None:
                    msgs.add_ai_message(str(last_ai_msg))
                    st.chat_message("ai").write(str(last_ai_msg))