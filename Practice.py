from mysql import connector
from dotenv import load_dotenv
import pandas as pd
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
import streamlit as st

DATABASE = "Redbus"

"""
This method is used to establish connection with MySQL and return the connection
"""
def connect_db():
    try:
        connection = connector.connect(
                host = "localhost",
                user = "root",
                password = '',
                database = DATABASE
            )
    except connector.Error as e:
        print(e)
    return connection

"""
Code snippet from filter_dataframe of StreamLit: 
https://blog.streamlit.io/auto-generate-a-dataframe-filtering-ui-in-streamlit-with-filter_dataframe/
"""
def filter_dataframe(df: pd.DataFrame):
    modify = st.checkbox("Add Filters")
    if not modify:
        return df
    
    df = df.copy()
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_timedelta(df[col])
            except Exception:
                pass
    modification_container = st.container()
    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            left.write("↳")
            # Treat columns with < 10 unique values as categorical
            if isinstance(df[column], pd.CategoricalDtype) or df[column].nunique() < 10:
                user_cat_input = right.multiselect(
                    f"Values for {column}",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                f"Values for {column}",
                min_value=_min,
                max_value=_max,
                value=(_min, _max),
                step=step,
                )
                df = df[df[column].between(*user_num_input)]
            elif is_datetime64_any_dtype(df[column]):
                user_date_input = right.date_input(
                    f"Values for {column}",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column].between(start_date, end_date)]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}",
                )
                if user_text_input:
                    df = df[df[column].astype(str).str.contains(user_text_input)]
    return df

st.title("Auto Filter Dataframes in Streamlit")
connection = connect_db()

# Read all the bus details from MySQL
df = pd.read_sql("SELECT * FROM bus_routes", connection)

# Update the time to "hh:mm:ss" format
df["departing_time"] = df["departing_time"].astype(str)
df["departing_time"] = df["departing_time"].str.split().str[2]
df["reaching_time"] = df["reaching_time"].astype(str)
df["reaching_time"] = df["reaching_time"].str.split().str[2]

# Load bus details to filter_dataframe
st.dataframe(filter_dataframe(df))