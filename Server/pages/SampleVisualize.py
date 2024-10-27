from microservices.gold_tables_graphs import Gold_Tables_Graphs
from Services.Critical_Services import CriticalServices
from dotenv import load_dotenv
import plotly.graph_objs as go
import streamlit as st
import os

load_dotenv("./Services/Airflow_Snowflake_Pipeline/snowflake.env", override=True)

USER_SNOWFLAKE = os.getenv("USER_SNOWFLAKE")
PASSWORD_SNOWFLAKE = os.getenv("PASSWORD_SNOWFLAKE")
ACCOUNT_SNOWFLAKE = os.getenv("ACCOUNT_SNOWFLAKE")
WAREHOUSE_SNOWFLAKE = os.getenv("WAREHOUSE_SNOWFLAKE")
DATABASE_SNOWFLAKE = os.getenv("DATABASE_SNOWFLAKE")
ROLE_SNOWFLAKE = os.getenv("ROLE_SNOWFLAKE")
URL_SNOWFLAKE = os.getenv("URL_SNOWFLAKE")

st.set_page_config(page_title="Sample Visualize", page_icon=":bar_chart:", layout="wide")

def page_layout():
    st.title("Sample Visualize")
    st.write("This page will display the visualizations for the Gold_MacroEconomic_Forecast table.")
    gold_table_data = retrieve_gold_table_task()
    if gold_table_data:
        graph_page = Graphs_Page()
        graph_page.UI(st.session_state.gold_table_data)
        #st.write(gold_table_data)
    else:
        st.write("Data could not be retrieved.")
    
@st.cache_data(ttl=600)
def retrieve_gold_table_task():
    try:
        graph_page = Graphs_Page()
        graph_page.run()
        return graph_page.load_data()
    except Exception as e:
        return None
    
class Graphs_Page:    
    def __init__(self):
        pass
    
    def run(self):
        self.declare_session_state()
    
    def declare_session_state(self):
        if "gold_table_data" not in st.session_state:
            st.session_state.gold_table_data = None
        if "searched_company" not in st.session_state:
            st.session_state.searched_company = None

    def load_data(self):
        try:
            critical_services_retrieve_data =  CriticalServices(USER_SNOWFLAKE, PASSWORD_SNOWFLAKE, ACCOUNT_SNOWFLAKE, WAREHOUSE_SNOWFLAKE, DATABASE_SNOWFLAKE, ROLE_SNOWFLAKE, URL_SNOWFLAKE).retrieve_gold_table_task()
            st.session_state.gold_table_data = critical_services_retrieve_data
            return critical_services_retrieve_data
        except Exception as e:
            return False
    
    def UI(self, data):
        gold_table_graphs = Gold_Tables_Graphs()
        #st.write(data)
        
        main_layout_col_1, main_layout_col_2 = st.columns([0.9, 0.3])
        with main_layout_col_1:
            main_layout_col_1_col_1, main_layout_col_1_col_2 = st.columns([0.5, 0.5])
            with main_layout_col_1_col_1:
                self.UI_Section_Actual_GDP(data)
        self.UI_Section_Forecasted_GDP(data)
        with main_layout_col_1_col_2:
            self.UI_Section_Forecasted_Inflation(data)
            self.UI_Section_Unemployment_Rate(data)
        
        with main_layout_col_2:
            list_of_dowjones_companies_section = st.container(height=350) 
            list_of_ipo_companies_section = st.container(height=250)
            
            company_name_input = st.text_input("Company Name", "Company Name")
            company_country_input = st.text_input("Country", "USA")
            search_button = st.button("Search")
            if search_button:
                st.session_state.searched_company = company_name_input
                st.write("Search button clicked")
        
        self.UI_Section_Time_Series(gold_table_graphs, data)
        
        
        
    
    def UI_Section_Actual_GDP(self, data):
        st.write("Actual GDP")
        dates = [row['DATETIME'] for row in data]
        gdp_gold = [row['GDP_GOLD'] for row in data]
        fig = go.Figure(data=[
            go.Scatter(x=dates, y=gdp_gold, mode='lines', name='Actual GDP')
        ], layout=go.Layout(
                width=800,
                height=760,
                xaxis=dict(title='Date'),
                yaxis=dict(title='GDP')
        ))
        st.plotly_chart(fig)
    
    def UI_Section_Forecasted_GDP(self, data):
        st.write("Forecasted GDP")
        dates = [row['DATETIME'] for row in data]
        forecasted_gdp = [row['FORECASTED_GDP'] for row in data]
        fig = go.Figure(data=[
            go.Scatter(x=dates, y=forecasted_gdp, mode='lines', name='Forecasted GDP')
        ], layout=go.Layout(
                width=1800,
                height=400,
                xaxis=dict(title='Date'),
                yaxis=dict(title='GDP')
        ))
        st.plotly_chart(fig)
    
    def UI_Section_Forecasted_Inflation(self, data):
        st.write("Forecasted Inflation")
        dates = [row['DATETIME'] for row in data]
        forecasted_inflation = [row['FORECASTED_INFLATION'] for row in data]
        fig = go.Figure(data=[
            go.Scatter(x=dates, y=forecasted_inflation, mode='lines', name='Forecasted Inflation')
        ], layout=go.Layout(
                width=800,
                height=350,
                xaxis=dict(title='Date'),
                yaxis=dict(title='Inflation')
        ))
        st.plotly_chart(fig)
        
    def UI_Section_Unemployment_Rate(self, data):
        st.write("Unemployment Rate")
        dates = [row['DATETIME'] for row in data]
        unemployment_rate = [row['UNEMPLOYMENT_RATE_GOLD'] for row in data]
        fig = go.Figure(data=[
            go.Scatter(x=dates, y=unemployment_rate, mode='lines', name='Unemployment Rate')
        ], layout=go.Layout(
                width=800,
                height=350,
                xaxis=dict(title='Date'),
                yaxis=dict(title='Unemployment Rate')
        ))
        st.plotly_chart(fig)
    
    def UI_Section_Time_Series(self, gold_table_graphs_, data):
        st.plotly_chart(gold_table_graphs_.plot_time_series(data))

page_layout()