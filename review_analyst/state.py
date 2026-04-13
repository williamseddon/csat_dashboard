from __future__ import annotations

import streamlit as st

from .config import DEFAULT_MODEL, DEFAULT_PRODUCT_URL, DEFAULT_REASONING, SOURCE_MODE_URL, TAB_DASHBOARD


def init_state() -> None:
    defaults = {
        'analysis_dataset': None,
        'shared_model': DEFAULT_MODEL,
        'shared_reasoning': DEFAULT_REASONING,
        'workspace_source_mode': SOURCE_MODE_URL,
        'workspace_product_url': DEFAULT_PRODUCT_URL,
        'workspace_product_urls_bulk': '',
        'workspace_file_uploader_nonce': 0,
        'workspace_active_tab': TAB_DASHBOARD,
        'workspace_tab_request': None,
        'review_explorer_page': 1,
        'review_explorer_per_page': 20,
        'review_explorer_sort': 'Newest',
        'ai_prompt': '',
        'ai_last_answer': '',
        'chat_messages': [],
        'chat_scope_signature': None,
        'chat_scope_notice': None,
        'ai_response_preset': 'Large (1200 words)',
        'ai_response_words': 1200,
        'rf_tf': 'All Time',
        'rf_sr': ['All'],
        'rf_kw': '',
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


def reset_workspace_state(reset_source: bool = True) -> None:
    st.session_state['analysis_dataset'] = None
    st.session_state['ai_prompt'] = ''
    st.session_state['ai_last_answer'] = ''
    st.session_state['chat_messages'] = []
    st.session_state['chat_scope_signature'] = None
    st.session_state['chat_scope_notice'] = None
    st.session_state['workspace_tab_request'] = None
    st.session_state['review_explorer_page'] = 1
    st.session_state['workspace_active_tab'] = TAB_DASHBOARD
    if reset_source:
        st.session_state['workspace_source_mode'] = SOURCE_MODE_URL
        st.session_state['workspace_product_url'] = DEFAULT_PRODUCT_URL
        st.session_state['workspace_product_urls_bulk'] = ''
        st.session_state['workspace_file_uploader_nonce'] = int(st.session_state.get('workspace_file_uploader_nonce', 0)) + 1
    reset_review_filters()


def reset_review_filters() -> None:
    for key in list(st.session_state.keys()):
        if key.startswith('rf_'):
            st.session_state.pop(key, None)
    st.session_state['review_explorer_page'] = 1
