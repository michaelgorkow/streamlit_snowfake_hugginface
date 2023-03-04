import streamlit as st
import json
from snowflake.snowpark.session import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
import pandas as pd

st.set_page_config(layout="wide")

connection_parameters = {
    "account": "",
    "user": "",
    "password": ""
}

# constants
numeric_types = [T.DecimalType, T.LongType, T.DoubleType, T.FloatType, T.IntegerType]
cat_types = [T.StringType]
dat_types = [T.TimestampType, T.DateType]

# functions
def load_data(database, schema, view):
    st.session_state.sf_dataframe = session.table(".".join([database,schema,view]))
    st.session_state.pd_dataframe = session.table(".".join([database,schema,view])).limit(1000).to_pandas()
    st.session_state.num_cols = [c.name for c in st.session_state.sf_dataframe.schema.fields if type(c.datatype) in numeric_types]
    st.session_state.cat_cols = [c.name for c in st.session_state.sf_dataframe.schema.fields if type(c.datatype) in cat_types]
    st.session_state.dat_cols = [c.name for c in st.session_state.sf_dataframe.schema.fields if type(c.datatype) in dat_types]


#Sidebar Login
sidebar = st.sidebar
with sidebar:
    st.title('Login to Snowflake')
    connection_parameters['account'] = st.text_input('Account:', '')
    connection_parameters['user'] = st.text_input('Username:', '')
    connection_parameters['password'] = st.text_input('Passwort:', '', type='password')
    login_button = st.button('Login')
    if login_button:
        # Creating Snowpark Session
        session = Session.builder.configs(connection_parameters).create()
        st.session_state.session = session
    if 'session' in st.session_state:
        session = st.session_state.session
        # Set role
        available_roles = pd.DataFrame(session.sql('SHOW ROLES').collect())
        selected_role = st.selectbox('Role:', available_roles['name'])
        session.use_role(selected_role)
        # Set warehouse
        available_warehouses = pd.DataFrame(session.sql('SHOW WAREHOUSES').collect())
        selected_warehouse = st.selectbox('Virtual Warehouse:', available_warehouses['name'])
        session.use_warehouse(selected_warehouse)
        # Select database
        available_databases = pd.DataFrame(session.sql('SHOW DATABASES').collect())
        selected_database = st.selectbox('Database:', available_databases['name'])
        session.use_database(selected_database)
        # Select Schema
        available_schemas = pd.DataFrame(session.sql('SHOW SCHEMAS').collect())
        selected_schema = st.selectbox('Schema:', available_schemas['name'])
        session.use_schema(selected_schema)
        # Select Table/View
        available_tables = pd.DataFrame(session.sql('SHOW TABLES').collect())
        available_views = pd.DataFrame(session.sql('SHOW VIEWS').collect())
        if (len(available_tables) > 0) and (len(available_views) > 0):
            available_views_tables = available_tables['name'].to_list() + available_views['name'].to_list()
        elif (len(available_tables) > 0) and (len(available_views) == 0):
            available_views_tables = available_tables['name'].to_list()
        elif (len(available_tables) == 0) and (len(available_views) > 0):
            available_views_tables = available_views['name'].to_list()
        if len(available_views_tables) > 0:
            selected_view_table = st.selectbox('Table / View:', available_views_tables)
            # Get Snowpark DataFrame
            #st.session_state.snowpark_df = session.table(".".join([selected_database,selected_schema,selected_view_table]))
            load_data(selected_database,selected_schema,selected_view_table)
        else:
            selected_table = st.selectbox('Table:', ['NO TABLE/VIEW AVAILABLE'])

data_view = st.expander('Data View', expanded=True)
with data_view:
    if 'pd_dataframe' in st.session_state:
        # Filter columns and display DataFrame
        selected_columns = st.multiselect('Select Columns:', st.session_state.sf_dataframe.columns, default=st.session_state.sf_dataframe.columns[0:10])
        df_view = st.dataframe(st.session_state.pd_dataframe, width=2000)
    else:
        help_text = st.info('No Data selected.')
        help_button = st.button("Help")
        if help_button:
            help_text.info('Please login and select a dataset from the left.')

data_visualisation = st.expander('Data Viz View', expanded=True)
with data_visualisation:
    if 'sf_dataframe' in st.session_state:
        col1, col2 = st.columns(2)
        with col1:
            y = st.selectbox("Y", st.session_state.num_cols)
            x = st.selectbox("X", st.session_state.num_cols+st.session_state.dat_cols)
        with col2:
            agg = st.selectbox("Aggregation", ['SUM','COUNT'])
        try:
            viz_dataframe = st.session_state.sf_dataframe.group_by(x).agg([(F.col(y),agg)]).limit(1000).to_pandas()
            st.dataframe(viz_dataframe)
            st.line_chart(viz_dataframe, x=viz_dataframe.columns[0], y=viz_dataframe.columns[1])
        except Exception as e:
            st.write(e)
    else:
        help_text = st.info('No Data selected.')
        help_button = st.button("Help ")
        if help_button:
            help_text.info('Please login and select a dataset from the left.')