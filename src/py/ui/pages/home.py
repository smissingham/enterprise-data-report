import streamlit as st
from ui.lib.page_utils import set_page_title
from lib.data import get_source_files, get_staging_files, get_output_files

set_page_title("🏠 Home")

st.markdown("""
Use the navigation menu above to explore different sections of the application.
""")

st.subheader("Current Data Files")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric(label="Source Data Files", value=len(get_source_files()))
with col2:
    st.metric(label="Staging Data Files", value=len(get_staging_files()))
with col3:
    st.metric(label="Output Data Files", value=len(get_output_files()))
st.divider()
