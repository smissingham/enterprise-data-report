import streamlit as st


def set_page_title(title: str):
    """Set the page title in the header row next to the refresh button."""
    if 'title_col' in st.session_state:
        with st.session_state.title_col:
            st.title(title)
    else:
        st.title(title)