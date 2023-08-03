import streamlit as st
import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

conn = st.experimental_connection("snowpark")

def get_databases():
    # Query Snowflake to get list of databases
    dbs = "SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES;"
    databases=conn.query(dbs)
    return databases


def get_schemas(database):
    # Query Snowflake to get list of schemas for the selected database
    sc = f" SELECT DISTINCT TABLE_SCHEMA FROM {database}.INFORMATION_SCHEMA.TABLES;"
    schemas=conn.query(sc)
    return schemas

def get_tables(database, schema):
    # Query Snowflake to get tables for the selected database and schema
    tabls = f"SELECT DISTINCT TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{schema}' UNION ALL SELECT DISTINCT TABLE_NAME FROM {database}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='{schema}';"
    tables=conn.query(tabls)
    return tables

def get_data(database, schema,table_name):
    # Query Snowflake to fetch data from the selected table
    query = f"SELECT * FROM {database}.{schema}.{table_name}   LIMIT 1000;"
    df = conn.query(query)
    return df

def show_data_profiles(df):
    # Display charts and data profiles for all columns in the DataFrame
    for column in df.columns:
        st.write(f"### Column: {column}")
        if df[column].dtype == "object":
            st.write("Data type: Categorical")
            st.bar_chart(df[column].value_counts())
        else:
            st.write("Data type: Numeric")
            st.write(df[column].describe())
            plt.figure(figsize=(8, 6))
            sns.histplot(df[column], kde=True)
            st.pyplot()

st.set_option('deprecation.showPyplotGlobalUse', False)



def main():
    st.title("KundaliofSnowTable - Snowflake Table Explorer")
    
    # Select the database
    selected_database = st.selectbox("Select Database", get_databases())

    # Select the schema
    selected_schema = st.selectbox("Select Schema", get_schemas(selected_database))

     # Select the table
    selected_table = st.selectbox("Select table", get_tables(selected_database, selected_schema))
  





    # Fetch data from Snowflake based on selected table
    df = get_data(selected_database, selected_schema,selected_table)

    # Display the table
    st.dataframe(df)

    # Show data profiles for all columns
    show_data_profiles(df)

if __name__ == "__main__":
    main()