import altair as alt
import pandas as pd
import streamlit as st
import os
from io import BytesIO
from azure.storage.blob import ContainerClient

# Show the page title and description.
st.set_page_config(page_title="US-50 Speed Lookup")
st.title("US-50 Speed Lookup")
st.write(
    """
    This app visualizes traffic speed data from US-50 in Maryland. It allows the user to search for the speeds of specific TMC segments over the 2024 year.
    """
)

def process_blob(storage_connection_string, container_name, blob_name):
    container = ContainerClient.from_connection_string(conn_str=storage_connection_string,
                                                       container_name=container_name)
    blob_client = container.get_blob_client(blob=blob_name)
    stream_downloader = blob_client.download_blob()
    stream = BytesIO()
    stream_downloader.readinto(stream)
    processed_df = pd.read_parquet(stream, engine='pyarrow')
    return processed_df

# Load the data from a parquet file. We're caching this so it doesn't reload every time the app
# reruns (e.g. if the user interacts with the widgets).
# @st.cache_data
def load_data():
    storage_connection_string = st.secrets['azure_datalakeus50_conn_string']

    container_name = 'speed'
    segments_blob = 'segments.parquet'
    summary_blob = 'summary.parquet'
    disaggregate_blob = 'disaggregate.parquet'

    disaggregate_df = process_blob(storage_connection_string, container_name, disaggregate_blob)

    df = pd.read_csv("data/movies_genres_summary.csv")
    return df


df = load_data()

# Show a multiselect widget with the genres using `st.multiselect`.
genres = st.multiselect(
    "Genres test",
    df.genre.unique(),
    ["Action", "Adventure"],
)

# Show a slider widget with the years using `st.slider`.
years = st.slider("Years", 1986, 2006, (2000, 2016))

# Filter the dataframe based on the widget input and reshape it.
df_filtered = df[(df["genre"].isin(genres)) & (df["year"].between(years[0], years[1]))]
df_reshaped = df_filtered.pivot_table(
    index="year", columns="genre", values="gross", aggfunc="sum", fill_value=0
)
df_reshaped = df_reshaped.sort_values(by="year", ascending=False)


# Display the data as a table using `st.dataframe`.
st.dataframe(
    df_reshaped,
    use_container_width=True,
    column_config={"year": st.column_config.TextColumn("Year")},
)

# Display the data as an Altair chart using `st.altair_chart`.
df_chart = pd.melt(
    df_reshaped.reset_index(), id_vars="year", var_name="genre", value_name="gross"
)
chart = (
    alt.Chart(df_chart)
    .mark_line()
    .encode(
        x=alt.X("year:N", title="Year"),
        y=alt.Y("gross:Q", title="Gross earnings ($)"),
        color="genre:N",
    )
    .properties(height=320)
)
st.altair_chart(chart, use_container_width=True)
