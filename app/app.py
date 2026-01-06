"""
Ultimate Reserve Projection - Actuarial Reserving Platform
Connects to SDP (Spark Declarative Pipeline) tables in Unity Catalog via SQL Connector
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime
import base64
import os

# SQL Connector for Databricks
try:
    from databricks import sql as databricks_sql
    SQL_CONNECTOR_AVAILABLE = True
except ImportError:
    SQL_CONNECTOR_AVAILABLE = False

# Configuration from environment
# Default to unified_reserves.losstriangle to match DLT pipeline target
CATALOG = os.getenv("CATALOG", "unified_reserves")
SCHEMA = os.getenv("SCHEMA", "losstriangle")
DATABRICKS_HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME", "")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")

def get_sql_connection():
    """Create a connection to Databricks SQL Warehouse"""
    if not SQL_CONNECTOR_AVAILABLE:
        return None
    if not DATABRICKS_HOST or not DATABRICKS_HTTP_PATH or not DATABRICKS_TOKEN:
        return None
    try:
        return databricks_sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
            _socket_timeout=30  # 30 second timeout
        )
    except Exception as e:
        return None

def run_sql_query(query, silent=True):
    """Execute a SQL query and return results as DataFrame"""
    conn = get_sql_connection()
    if conn is None:
        return None
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
            return None
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except:
            pass

# Check if we can connect to real data
USE_REAL_DATA = SQL_CONNECTOR_AVAILABLE and DATABRICKS_HOST and DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN

st.set_page_config(page_title="Ultimate Reserve Projection", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
    .stApp { background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%); }
    section[data-testid="stSidebar"] { background: linear-gradient(180deg, #f5f5f0 0%, #e8e8e0 100%); }
    section[data-testid="stSidebar"] * { color: #1e3a5f !important; }
    section[data-testid="stSidebar"] h1, section[data-testid="stSidebar"] h2, section[data-testid="stSidebar"] h3, section[data-testid="stSidebar"] h4 { color: #1e3a5f !important; }
    section[data-testid="stSidebar"] p, section[data-testid="stSidebar"] span, section[data-testid="stSidebar"] label { color: #1e3a5f !important; }
    section[data-testid="stSidebar"] .stRadio > div { color: #1e3a5f !important; }
    section[data-testid="stSidebar"] .stRadio label { color: #1e3a5f !important; font-weight: 500; }
    section[data-testid="stSidebar"] .stRadio label p { color: #1e3a5f !important; }
    section[data-testid="stSidebar"] .stRadio label span { color: #1e3a5f !important; }
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label { color: #1e3a5f !important; }
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label p { color: #1e3a5f !important; }
    section[data-testid="stSidebar"] [data-baseweb="radio"] + div { color: #1e3a5f !important; }
    section[data-testid="stSidebar"] [data-baseweb="radio"] + div p { color: #1e3a5f !important; }
    section[data-testid="stSidebar"] div[data-testid="stMarkdownContainer"] { color: #1e3a5f !important; }
    section[data-testid="stSidebar"] div[data-testid="stMarkdownContainer"] p { color: #1e3a5f !important; }
    .main-title { background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2rem; font-weight: 700; }
    .section-header { color: #1e3a5f; font-size: 1.25rem; font-weight: 600; padding: 0.75rem 0; border-bottom: 2px solid #2d5a87; margin-bottom: 1rem; }
    .stButton > button { background: linear-gradient(135deg, #2d5a87 0%, #1e3a5f 100%); color: white; border: none; border-radius: 8px; padding: 0.5rem 1.5rem; font-weight: 600; }
    .ai-output { background: #ecfdf5; border: 1px solid #10b981; border-radius: 12px; padding: 1rem; color: #065f46; }
    #MainMenu {visibility: hidden;} footer {visibility: hidden;}
    .logo-container { text-align: center; padding: 1rem; background: rgba(255, 255, 255, 0.7); border-radius: 12px; margin-bottom: 1rem; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
    .data-source { background: #fef3c7; border: 1px solid #f59e0b; border-radius: 8px; padding: 0.5rem 1rem; margin: 0.5rem 0; color: #92400e; font-size: 0.85rem; }
</style>
""", unsafe_allow_html=True)


def get_logo_html():
    possible_paths = [os.path.join(os.path.dirname(__file__), 'logo.png'), '/app/python/source_code/logo.png', 'logo.png']
    for logo_path in possible_paths:
        if os.path.exists(logo_path):
            with open(logo_path, 'rb') as f:
                logo_data = base64.b64encode(f.read()).decode()
            return f'<img src="data:image/png;base64,{logo_data}" style="max-width: 100%; height: auto;">'
    return '<div style="text-align: center; padding: 10px;"><h3 style="color: white; margin: 0;">CLAIM WISE INSURANCE</h3></div>'


# SDP Table mappings - all tables in unified_reserves.losstriangle
# DLT uses single schema for all tables (bronze_, silver_, gold_ prefixes for organization)
SDP_TABLES = {
    'triangle': 'silver_triangle_pivot',
    'reserves': 'gold_reserve_estimates',  # Direct table from chain_ladder_job.py
    'dev_factors': 'gold_development_factors_python',  # Direct table from chain_ladder_job.py
    'risk': 'gold_v_risk_exposure',
    'watchlist': 'gold_v_claims_watchlist',
    'executive': 'gold_v_executive_dashboard',
    'loss_types': 'silver_triangle_by_loss_type'
}

def get_table_path(table_key):
    """Get full table path for SDP table"""
    return f"{CATALOG}.{SCHEMA}.{SDP_TABLES[table_key]}"

@st.cache_data(ttl=300)
def load_triangle_data(loss_type='ALL'):
    if USE_REAL_DATA:
        if loss_type == 'ALL':
            df = run_sql_query(f"SELECT * FROM {get_table_path('triangle')} ORDER BY origin_period", silent=True)
        else:
            # Use silver_triangle_by_loss_type for specific loss types and pivot it
            df = run_sql_query(f"""
                SELECT origin_period,
                    MAX(CASE WHEN development_period = 0 THEN cumulative_value END) AS dev_0,
                    MAX(CASE WHEN development_period = 1 THEN cumulative_value END) AS dev_1,
                    MAX(CASE WHEN development_period = 2 THEN cumulative_value END) AS dev_2,
                    MAX(CASE WHEN development_period = 3 THEN cumulative_value END) AS dev_3,
                    MAX(CASE WHEN development_period = 4 THEN cumulative_value END) AS dev_4,
                    MAX(CASE WHEN development_period = 5 THEN cumulative_value END) AS dev_5,
                    MAX(CASE WHEN development_period = 6 THEN cumulative_value END) AS dev_6,
                    MAX(CASE WHEN development_period = 7 THEN cumulative_value END) AS dev_7,
                    MAX(CASE WHEN development_period = 8 THEN cumulative_value END) AS dev_8,
                    MAX(CASE WHEN development_period = 9 THEN cumulative_value END) AS dev_9
                FROM {get_table_path('loss_types')}
                WHERE loss_type = '{loss_type}'
                GROUP BY origin_period
                ORDER BY origin_period
            """, silent=True)
        if df is not None and not df.empty:
            return df
    return generate_sample_triangle_data(loss_type)


@st.cache_data(ttl=300)
def load_reserve_estimates(loss_type='ALL'):
    if USE_REAL_DATA:
        # Query gold_reserve_estimates directly (written by chain_ladder_job.py)
        # Column mapping: origin_period->origin_year, latest_cumulative->paid_to_date, 
        #                 ultimate_loss->projected_ultimate, ibnr->estimated_ibnr, percent_reported->percent_developed
        if loss_type == 'ALL':
            df = run_sql_query(f"""
                SELECT 
                    origin_period AS origin_year, 
                    loss_type, 
                    latest_cumulative AS paid_to_date, 
                    ultimate_loss AS projected_ultimate, 
                    ibnr AS estimated_ibnr, 
                    ROUND(percent_reported * 100, 1) AS percent_developed 
                FROM {get_table_path('reserves')} 
                WHERE loss_type = 'ALL'
                ORDER BY origin_period
            """, silent=True)
        else:
            df = run_sql_query(f"""
                SELECT 
                    origin_period AS origin_year, 
                    loss_type, 
                    latest_cumulative AS paid_to_date, 
                    ultimate_loss AS projected_ultimate, 
                    ibnr AS estimated_ibnr, 
                    ROUND(percent_reported * 100, 1) AS percent_developed 
                FROM {get_table_path('reserves')} 
                WHERE loss_type = '{loss_type}' 
                ORDER BY origin_period
            """, silent=True)
        if df is not None and not df.empty:
            return df
    return generate_sample_actuarial_data(loss_type)


@st.cache_data(ttl=300)
def load_development_factors(loss_type='ALL'):
    if USE_REAL_DATA:
        # Query gold_development_factors_python directly (written by chain_ladder_job.py)
        df = run_sql_query(f"""
            SELECT development_period, ata_factor, cdf_to_ultimate, percent_developed 
            FROM {get_table_path('dev_factors')} 
            WHERE loss_type = '{loss_type}'
            ORDER BY development_period
        """, silent=True)
        if df is not None and not df.empty:
            return df
    return None


@st.cache_data(ttl=300)
def load_risk_summary():
    if USE_REAL_DATA:
        df = run_sql_query(f"SELECT origin_year, loss_type, total_claims, high_risk_claims, high_risk_percentage as high_risk_pct, total_incurred, avg_risk_score FROM {get_table_path('risk')} ORDER BY origin_year", silent=True)
        if df is not None and not df.empty:
            return df
    return None


@st.cache_data(ttl=300)
def load_claims_watchlist():
    if USE_REAL_DATA:
        df = run_sql_query(f"SELECT claim_id, loss_type, risk_score, incurred_amount FROM {get_table_path('watchlist')} LIMIT 100", silent=True)
        if df is not None and not df.empty:
            return df
    return None


@st.cache_data(ttl=300)
def load_executive_dashboard():
    if USE_REAL_DATA:
        df = run_sql_query(f"SELECT * FROM {get_table_path('executive')}", silent=True)
        if df is not None and not df.empty:
            return df
    return None


@st.cache_data(ttl=300)
def get_loss_types():
    if USE_REAL_DATA:
        df = run_sql_query(f"SELECT DISTINCT loss_type FROM {get_table_path('loss_types')} WHERE loss_type != 'ALL' ORDER BY loss_type", silent=True)
        if df is not None and not df.empty:
            return df['loss_type'].tolist()
    return ['Water Damage', 'Wind', 'Fire', 'Theft', 'Liability']


def generate_sample_triangle_data(loss_type='ALL'):
    # Different base values for different loss types
    loss_type_multipliers = {'ALL': 1.0, 'Fire': 1.5, 'Water Damage': 0.8, 'Wind': 1.2, 'Theft': 0.5, 'Liability': 2.0}
    multiplier = loss_type_multipliers.get(loss_type, 1.0)
    
    origin_years = list(range(2018, 2024))
    data = []
    for oy in origin_years:
        row = {'origin_period': oy}
        base = int((50000 + (oy - 2018) * 10000) * multiplier)
        cumulative = base
        max_dev = 2023 - oy
        for d in range(10):
            row[f'dev_{d}'] = cumulative if d <= max_dev else None
            if d <= max_dev and d > 0:
                cumulative = int(cumulative * 1.15)
        data.append(row)
    return pd.DataFrame(data)


def generate_sample_actuarial_data(loss_type='ALL'):
    # Different base values for different loss types
    loss_type_multipliers = {'ALL': 1.0, 'Fire': 1.5, 'Water Damage': 0.8, 'Wind': 1.2, 'Theft': 0.5, 'Liability': 2.0}
    multiplier = loss_type_multipliers.get(loss_type, 1.0)
    
    origin_years = list(range(2018, 2024))
    data = []
    for oy in origin_years:
        paid = int((100000 + (2023 - oy) * 50000) * multiplier)
        ultimate = paid * (1 + (2023 - oy) * 0.1)
        ibnr = ultimate - paid
        data.append({'origin_year': oy, 'loss_type': loss_type, 'paid_to_date': paid, 'projected_ultimate': ultimate, 'estimated_ibnr': max(0, ibnr), 'percent_developed': 100 / (1 + (2023 - oy) * 0.1)})
    return pd.DataFrame(data)


if 'chain_ladder_weights' not in st.session_state:
    st.session_state.chain_ladder_weights = {}
if 'submissions' not in st.session_state:
    st.session_state.submissions = []
if 'user_comments' not in st.session_state:
    st.session_state.user_comments = {}
if 'additional_reserves' not in st.session_state:
    st.session_state.additional_reserves = {}
if 'credibility' not in st.session_state:
    st.session_state.credibility = {}


def create_triangle_heatmap(triangle_df, title="Loss Triangle"):
    origin_years = triangle_df['origin_period'].tolist()
    dev_cols = [c for c in triangle_df.columns if c.startswith('dev_')]
    z_data = triangle_df[dev_cols].values.tolist()
    fig = go.Figure(data=go.Heatmap(z=z_data, x=[c.replace('dev_', '') for c in dev_cols], y=[str(oy) for oy in origin_years], colorscale=[[0, '#1e3a5f'], [0.5, '#3d7ab5'], [1, '#d4e6f1']], text=[[f'${v:,.0f}' if v and not pd.isna(v) else '' for v in row] for row in z_data], texttemplate='%{text}', textfont={"size": 10, "color": "white"}, showscale=True))
    fig.update_layout(title=dict(text=title, font=dict(size=14, color='#1e3a5f')), xaxis_title="Development Period", yaxis_title="Origin Year", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', height=400)
    fig.update_yaxes(autorange="reversed")
    return fig


def create_reserve_comparison_chart(df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['origin_year'], y=df['paid_to_date'], mode='lines+markers', name='Paid', line=dict(color='#2d5a87', width=3)))
    fig.add_trace(go.Scatter(x=df['origin_year'], y=df['projected_ultimate'], mode='lines+markers', name='Ultimate', line=dict(color='#dc2626', width=3)))
    fig.add_trace(go.Bar(x=df['origin_year'], y=df['estimated_ibnr'], name='IBNR', marker_color='rgba(16,185,129,0.7)'))
    fig.update_layout(title="Reserve Analysis", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', height=400, yaxis_tickformat='$,.0f', barmode='overlay')
    return fig


def main():
    with st.sidebar:
        logo_html = get_logo_html()
        st.markdown(f'<div class="logo-container">{logo_html}</div>', unsafe_allow_html=True)
        st.markdown("---")
        if USE_REAL_DATA:
            st.markdown(f"📊 **Data:** `{CATALOG}.{SCHEMA}`")
        else:
            st.markdown("⚠️ **Sample Data Mode**")
        st.markdown(f'<p style="font-size: 0.8rem; color: #1e3a5f;">Valuation: {datetime.now().strftime("%B %d, %Y")}</p>', unsafe_allow_html=True)
        
        # Add refresh button to clear cache
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # Navigation at top of main area
    nav_col1, nav_col2, nav_col3 = st.columns([1, 1, 2])
    with nav_col1:
        proj_btn = st.button("🎯 Ultimate Reserve Projection", key="nav_proj", use_container_width=True)
    with nav_col2:
        exec_btn = st.button("📊 Executive Reporting", key="nav_exec", use_container_width=True)
    
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "projection"
    
    if proj_btn:
        st.session_state.current_page = "projection"
    if exec_btn:
        st.session_state.current_page = "executive"
    
    st.markdown("---")
    
    if st.session_state.current_page == "projection":
        render_reserve_projection()
    else:
        render_executive_reporting()


def render_reserve_projection():
    st.markdown('<h1 class="main-title">Ultimate Reserve Projection</h1>', unsafe_allow_html=True)
    
    if USE_REAL_DATA:
        st.markdown(f'<div class="data-source">📊 Data from <b>{CATALOG}.{SCHEMA}</b> (SDP Pipeline)</div>', unsafe_allow_html=True)
    else:
        # Show connection debug info
        missing = []
        if not SQL_CONNECTOR_AVAILABLE:
            missing.append("SQL Connector not installed")
        if not DATABRICKS_HOST:
            missing.append("DATABRICKS_SERVER_HOSTNAME not set")
        if not DATABRICKS_HTTP_PATH:
            missing.append("DATABRICKS_HTTP_PATH not set")
        if not DATABRICKS_TOKEN:
            missing.append("DATABRICKS_TOKEN not set")
        debug_msg = ", ".join(missing) if missing else "Connection test failed"
        st.markdown(f'<div class="data-source">⚠️ Using sample data ({debug_msg})</div>', unsafe_allow_html=True)
    
    st.markdown('<div class="section-header">📁 Claim and Payment Data</div>', unsafe_allow_html=True)
    
    loss_types = get_loss_types()
    
    col1, col2 = st.columns([1, 1])
    with col1:
        selected_loss_type = st.selectbox("Loss Type", ['ALL'] + loss_types, key="loss_type_select")
    with col2:
        st.markdown(f"**Selected:** {selected_loss_type}")
    
    # Load data filtered by loss type
    triangle_df = load_triangle_data(selected_loss_type)
    reserve_df = load_reserve_estimates(selected_loss_type)
    
    tri_col1, tri_col2 = st.columns([2, 1])
    with tri_col1:
        fig1 = create_triangle_heatmap(triangle_df, f"Loss Triangle - {selected_loss_type}")
        st.plotly_chart(fig1, use_container_width=True)
    with tri_col2:
        fig2 = create_reserve_comparison_chart(reserve_df)
        st.plotly_chart(fig2, use_container_width=True)
    
    st.markdown("---")
    st.markdown('<div class="section-header">📐 Actuarial Reserving</div>', unsafe_allow_html=True)
    
    dev_factors = load_development_factors(selected_loss_type)
    
    # Show Development Factors first
    if dev_factors is not None and not dev_factors.empty:
        st.markdown("##### Development Factors (SDP)")
        st.dataframe(dev_factors.head(10), use_container_width=True, hide_index=True)
        st.markdown("---")
    
    # Initialize credibility values for each origin year
    for _, row in reserve_df.iterrows():
        oy = row['origin_year']
        if oy not in st.session_state.credibility:
            st.session_state.credibility[oy] = 0.5
    
    # Calculate Bornhuetter-Ferguson Ultimate
    # BF converges to ChainLadder in early years (high % developed)
    # BF differs from ChainLadder in recent years (low % developed)
    def calculate_bf_ultimate(chain_ladder, percent_developed, paid_to_date):
        """Calculate Bornhuetter-Ferguson Ultimate that converges to ChainLadder in early years"""
        # For early years (high % developed), BF should converge to ChainLadder
        # For recent years (low % developed), BF starts lower/higher than ChainLadder
        # Use a deviation factor that decreases as % developed increases
        deviation_factor = -0.12  # 12% lower for very recent years (conservative)
        convergence_factor = percent_developed / 100.0  # 0 to 1
        
        # BF starts lower than ChainLadder for recent years and converges to ChainLadder for early years
        # For recent years (low % developed): BF = ChainLadder * (1 + deviation_factor)
        # For early years (high % developed): BF = ChainLadder
        adjustment = deviation_factor * (1 - convergence_factor)
        bf_ultimate = chain_ladder * (1 + adjustment)
        
        # Ensure BF doesn't go below paid amount
        bf_ultimate = max(bf_ultimate, paid_to_date * 1.1)
        
        return bf_ultimate
    
    # Prepare data for Reserve Estimates table
    st.markdown("**Reserve Estimates by Origin Year**")
    
    # Create input columns for credibility
    reserve_data = []
    for idx, row in reserve_df.iterrows():
        oy = row['origin_year']
        paid = row['paid_to_date']
        chain_ladder_ultimate = row['projected_ultimate']
        ibnr = row['estimated_ibnr']
        percent_dev = row.get('percent_developed', 100)
        
        # Case Reserve set to flat value
        case_reserve = 99
        
        # Calculate Bornhuetter-Ferguson Ultimate
        bf_ultimate = calculate_bf_ultimate(chain_ladder_ultimate, percent_dev, paid)
        
        # Get credibility (default 0.5 if not set)
        credibility_val = st.session_state.credibility.get(oy, 0.5)
        
        # Calculate Ultimate Reserve: credibility*chain_ladder + (1-credibility)*bf
        ultimate_reserve = credibility_val * chain_ladder_ultimate + (1 - credibility_val) * bf_ultimate
        
        # Calculate IBNR for ultimate reserve
        ibnr_reserve = ultimate_reserve - paid - case_reserve
        
        reserve_data.append({
            'origin_year': oy,
            'paid_to_date': paid,
            'case_reserve': case_reserve,
            'chain_ladder_ultimate': chain_ladder_ultimate,
            'bf_ultimate': bf_ultimate,
            'credibility': credibility_val,
            'ultimate_reserve': ultimate_reserve,
            'ibnr': ibnr_reserve
        })
    
    # Create table with editable credibility
    col_table, col_chart = st.columns([1.2, 1])
    
    with col_table:
        # Add header row first
        h1, h2, h3, h4, h5, h6, h7, h8 = st.columns([1, 1.2, 1.2, 1.2, 1.2, 1, 1.2, 1.2])
        with h1:
            st.markdown("**Origin Year**")
        with h2:
            st.markdown("**Paid To Date**")
        with h3:
            st.markdown("**Case Reserve**")
        with h4:
            st.markdown("**Ultimate (ChainLadder)**")
        with h5:
            st.markdown("**Ultimate (BF)**")
        with h6:
            st.markdown("**Credibility**")
        with h7:
            st.markdown("**Ultimate Reserve**")
        with h8:
            st.markdown("**IBNR**")
        
        st.markdown("<hr style='margin: 0.5rem 0;'>", unsafe_allow_html=True)
        
        # Create a form-like interface for credibility inputs
        for data in reserve_data:
            oy = data['origin_year']
            col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([1, 1.2, 1.2, 1.2, 1.2, 1, 1.2, 1.2])
            with col1:
                st.markdown(f"**{oy}**")
            with col2:
                st.markdown(f"${data['paid_to_date']:,.0f}")
            with col3:
                st.markdown(f"${data['case_reserve']:,.0f}")
            with col4:
                st.markdown(f"${data['chain_ladder_ultimate']:,.0f}")
            with col5:
                st.markdown(f"${data['bf_ultimate']:,.0f}")
            with col6:
                credibility_input = st.number_input(
                    "",
                    min_value=0.0,
                    max_value=1.0,
                    value=float(data['credibility']),
                    step=0.1,
                    key=f"cred_{oy}",
                    label_visibility="collapsed"
                )
                # Update session state immediately
                st.session_state.credibility[oy] = credibility_input
            with col7:
                # Recalculate ultimate reserve with current credibility input
                new_ultimate_reserve = credibility_input * data['chain_ladder_ultimate'] + (1 - credibility_input) * data['bf_ultimate']
                st.markdown(f"**${new_ultimate_reserve:,.0f}**")
            with col8:
                new_ibnr = new_ultimate_reserve - data['paid_to_date'] - data['case_reserve']
                st.markdown(f"${new_ibnr:,.0f}")
    
    with col_chart:
        # Update chart with new ultimate reserves based on current credibility
        fig = go.Figure()
        fig.add_trace(go.Bar(x=reserve_df['origin_year'], y=reserve_df['paid_to_date'], name='Paid', marker_color='rgba(45,90,135,0.7)'))
        # Recalculate IBNR using current credibility values from session_state
        updated_ibnr = []
        for data in reserve_data:
            oy = data['origin_year']
            current_cred = st.session_state.credibility.get(oy, data['credibility'])
            current_ultimate = current_cred * data['chain_ladder_ultimate'] + (1 - current_cred) * data['bf_ultimate']
            current_ibnr = current_ultimate - data['paid_to_date'] - data['case_reserve']
            updated_ibnr.append(current_ibnr)
        fig.add_trace(go.Bar(x=reserve_df['origin_year'], y=updated_ibnr, name='IBNR', marker_color='rgba(220,38,38,0.7)'))
        fig.update_layout(title="Paid vs IBNR", barmode='stack', paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', yaxis_tickformat='$,.0f', height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    st.markdown('<div class="section-header">🤖 AI Risk Analyzer</div>', unsafe_allow_html=True)
    
    risk_summary = load_risk_summary()
    claims_watchlist = load_claims_watchlist()
    
    # Initialize session state for button toggles
    if 'show_high_risk_claims' not in st.session_state:
        st.session_state.show_high_risk_claims = False
    if 'show_risk_summary' not in st.session_state:
        st.session_state.show_risk_summary = False
    
    # Two buttons stacked vertically
    btn_col1, btn_col2, _ = st.columns([1, 1, 2])
    with btn_col1:
        if st.button("⚠️ High-Risk Claims", key="btn_high_risk", use_container_width=True):
            st.session_state.show_high_risk_claims = not st.session_state.show_high_risk_claims
            st.session_state.show_risk_summary = False  # Hide the other table
    with btn_col2:
        if st.button("📊 Risk Summary (SDP NLP)", key="btn_risk_summary", use_container_width=True):
            st.session_state.show_risk_summary = not st.session_state.show_risk_summary
            st.session_state.show_high_risk_claims = False  # Hide the other table
    
    # Show High-Risk Claims table when button is pressed
    if st.session_state.show_high_risk_claims:
        if claims_watchlist is not None and not claims_watchlist.empty:
            st.markdown("**High-Risk Claims**")
            st.dataframe(claims_watchlist.head(10), use_container_width=True, hide_index=True)
        else:
            st.info("Run SDP pipeline for claims watchlist.")
    
    # Show Risk Summary table when button is pressed
    if st.session_state.show_risk_summary:
        if risk_summary is not None and not risk_summary.empty:
            st.markdown("**Risk Summary (SDP NLP)**")
            st.dataframe(risk_summary, use_container_width=True, hide_index=True)
        else:
            st.info("Run SDP pipeline for risk analysis.")
    
    st.markdown("---")
    st.markdown('<div class="section-header">📤 Submission</div>', unsafe_allow_html=True)
    
    h1, h2, h3, h4, h5 = st.columns([1, 1, 1, 1, 2])
    with h1:
        st.markdown("**Origin Year**")
    with h2:
        st.markdown("**Ultimate**")
    with h3:
        st.markdown("**Additional**")
    with h4:
        st.markdown("**Total**")
    with h5:
        st.markdown("**Comments**")
    
    st.markdown("<hr style='margin: 0.5rem 0;'>", unsafe_allow_html=True)
    
    sub_data = []
    for _, row in reserve_df.iterrows():
        oy = row['origin_year']
        ultimate = row['projected_ultimate']
        add = st.session_state.additional_reserves.get(oy, 0)
        sub_data.append({'OY': oy, 'Ultimate': ultimate, 'Additional': add, 'Total': ultimate + add})
    
    for s in sub_data:
        c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 2])
        with c1:
            st.markdown(f"**{s['OY']}**")
        with c2:
            st.markdown(f"${s['Ultimate']:,.0f}")
        with c3:
            st.markdown(f"${s['Additional']:,.0f}")
        with c4:
            st.markdown(f"**${s['Total']:,.0f}**")
        with c5:
            st.session_state.user_comments[s['OY']] = st.text_input("", value=st.session_state.user_comments.get(s['OY'], ''), key=f"c_{s['OY']}", label_visibility="collapsed")
    
    if st.button("📤 Submit", key="sub"):
        total_reserve = sum(s['Total'] for s in sub_data)
        st.session_state.submissions.append({'Datetime': datetime.now().strftime('%m/%d/%Y %H:%M'), 'Loss Type': selected_loss_type, 'Total Reserve': f"${total_reserve:,.0f}"})
        st.success(f"✅ Submitted!")
    
    if st.session_state.submissions:
        st.markdown("##### Submission History")
        st.dataframe(pd.DataFrame(st.session_state.submissions), use_container_width=True, hide_index=True)


def render_executive_reporting():
    st.markdown('<h1 class="main-title">Executive Reporting</h1>', unsafe_allow_html=True)
    
    # Load reserve data (this works from SDP pipeline)
    reserve_df = load_reserve_estimates('ALL')
    
    if reserve_df is not None and not reserve_df.empty:
        total_paid = reserve_df['paid_to_date'].sum()
        total_ibnr = reserve_df['estimated_ibnr'].sum()
        total_ultimate = reserve_df['projected_ultimate'].sum()
        avg_developed = reserve_df['percent_developed'].mean() if 'percent_developed' in reserve_df.columns else 0
        
        if USE_REAL_DATA:
            st.markdown(f'<div class="data-source">📊 Data from <b>{CATALOG}.{SCHEMA}</b> (SDP Pipeline)</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="data-source">⚠️ Using sample data</div>', unsafe_allow_html=True)
        
        # Key metrics
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Total Paid", f"${total_paid/1e6:.2f}M")
        with c2:
            st.metric("Total IBNR", f"${total_ibnr/1e6:.2f}M")
        with c3:
            st.metric("Total Ultimate", f"${total_ultimate/1e6:.2f}M")
        with c4:
            st.metric("Avg % Developed", f"{avg_developed:.1f}%")
        
        st.markdown("---")
        
        # Reserve breakdown by loss type
        st.markdown("### Reserve Summary by Loss Type")
        loss_types = get_loss_types()
        
        summary_data = []
        for lt in ['ALL'] + loss_types[:5]:
            lt_df = load_reserve_estimates(lt)
            if lt_df is not None and not lt_df.empty:
                summary_data.append({
                    'Loss Type': lt,
                    'Total Paid': f"${lt_df['paid_to_date'].sum():,.0f}",
                    'Total IBNR': f"${lt_df['estimated_ibnr'].sum():,.0f}",
                    'Total Ultimate': f"${lt_df['projected_ultimate'].sum():,.0f}",
                    'Origin Years': len(lt_df)
                })
        
        if summary_data:
            st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # Chart
        st.markdown("### Reserve Analysis (All Loss Types)")
        fig = create_reserve_comparison_chart(reserve_df)
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed table
        st.markdown("### Detailed Reserve Estimates")
        st.dataframe(reserve_df, use_container_width=True, hide_index=True)
    else:
        st.warning("No reserve data available. Please run the SDP pipeline.")


if __name__ == "__main__":
    main()
