import streamlit as st
import polars as pl
from pygwalker.api.streamlit import StreamlitRenderer
from lib.settings import get_setting, Setting
from lib.data import get_staging_files, get_output_files, read_dataframe
from ui.lib.page_utils import set_page_title
import warnings

# Hide all warnings from the Streamlit interface
st.set_option("client.showErrorDetails", False)
warnings.simplefilter("ignore")

# Page Title & Contents
set_page_title("📊 Data Explorer")
st.write("Select which data set to explore")

# Data selection controls
type_col, source_col = st.columns(2)
with type_col:
    data_type = st.selectbox("Type", ["Staging", "Output"], index=0)
with source_col:
    if data_type == "Staging":
        available_files = get_staging_files()
    else:
        available_files = get_output_files()

    if available_files:
        selected_file = st.selectbox("Source", available_files, index=0)
    else:
        st.warning(f"No readable files found in {data_type} directory")
        selected_file = None

# Load dataframe for selected file
if selected_file:
    st.write(f"Debug: Loading {data_type} - {selected_file}")
    df = read_dataframe(data_type, selected_file)

    # Display data
    if df is not None and selected_file:
        st.subheader("PyGWalker Data Visualisation")

        # Create unique key for PyGWalker to force re-render on selection change
        walker_key = f"{data_type}_{selected_file}"

        pyg_app = StreamlitRenderer(df)
        pyg_app.explorer(key=walker_key)

        st.subheader("Aggregate Summary")
        st.write(df.describe())
    elif selected_file:
        st.error(f"Failed to load {selected_file}")
    else:
        st.info("Please select a file to explore")
