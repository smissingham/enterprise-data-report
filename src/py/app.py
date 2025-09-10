import os
import glob

import streamlit as st
import pandas as pd
import polars as pl
from pygwalker.api.streamlit import StreamlitRenderer

from lib.stage import stagefiles_ensure, stagefiles_refresh


def run_pyg(df: pl.DataFrame):
    pyg_app = StreamlitRenderer(df)
    pyg_app.explorer()


if __name__ == "__main__":
    stagefiles_ensure()

    st.set_page_config(
        page_title="Enterprise Data Report",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    # Define pages with proper navigation
    pages = {
        "Main": [
            st.Page("ui/pages/home.py", title="Home", icon="🏠"),
        ],
        "Analysis": [
            st.Page("ui/pages/explorer.py", title="Data Explorer", icon="📊"),
        ],
    }

    # Create header with title and refresh button on same line
    col1, col2 = st.columns([2, 1])

    # Store columns in session state for pages to use
    st.session_state.title_col = col1
    st.session_state.refresh_col = col2

    with col2:
        st.markdown("<div style='padding-top: 1rem;'></div>", unsafe_allow_html=True)
        if st.button(
            "🔄 Refresh Data", type="secondary", help="Refresh all data sources"
        ):
            with st.spinner("Refreshing data..."):
                stagefiles_refresh()
            st.success("Data refreshed successfully!", icon="✅")
            st.rerun()

    # Create navigation with top position
    pg = st.navigation(pages, position="top")
    pg.run()
