"""
AdTech Fraud Detection Platform - Streamlit Dashboard
======================================================
Purpose: Interactive monitoring and analysis of fraud detection results
Author: Nicolas Zalazar
Version: 2.0 (Unified Platform)
Date: 2026-02-28

Features:
- Real-time alert monitoring
- Geographic fraud distribution
- Model performance metrics
- Historical trend analysis
- Alert management interface
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="AdTech Fraud Detection Platform",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #6b7280;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 12px;
        padding: 20px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .alert-critical {
        background-color: #fee2e2;
        border-left: 4px solid #ef4444;
        padding: 15px;
        margin: 10px 0;
        border-radius: 4px;
    }
    .alert-high {
        background-color: #fef3c7;
        border-left: 4px solid #f59e0b;
        padding: 15px;
        margin: 10px 0;
        border-radius: 4px;
    }
    .stMetric {
        background-color: #f9fafb;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #e5e7eb;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================================================
# HEADER
# ============================================================================

st.markdown('<p class="main-header">🛡️ AdTech Fraud Detection Platform</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Enterprise-Grade Fraud Detection | Real-Time Monitoring | 10TB+ Scale</p>', unsafe_allow_html=True)
st.markdown("---")

# ============================================================================
# SIDEBAR - NAVIGATION & FILTERS
# ============================================================================

with st.sidebar:
    st.markdown("### 🎯 Navigation")
    
    page = st.radio(
        "Select View:",
        ["📊 Overview", "🚨 Active Alerts", "🌍 Geographic Analysis", "📈 Trends", "⚙️ Model Performance"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    st.markdown("### 🔧 Filters")
    
    # Date range filter
    date_range = st.date_input(
        "Date Range",
        value=(datetime.now() - timedelta(days=7), datetime.now())
    )
    
    # Risk level filter
    risk_levels = st.multiselect(
        "Risk Levels",
        ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
        default=["CRITICAL", "HIGH"]
    )
    
    # Action filter
    actions = st.multiselect(
        "Actions",
        ["BLOCK_IP_IMMEDIATELY", "SEND_TO_MANUAL_REVIEW", "MONITOR_PASSIVELY"],
        default=["BLOCK_IP_IMMEDIATELY", "SEND_TO_MANUAL_REVIEW"]
    )
    
    st.markdown("---")
    st.markdown("### 📋 Quick Stats")
    
    # Simulated quick stats (in production, query from Snowflake)
    st.metric("Total Alerts (24h)", "1,247", "+12%")
    st.metric("Critical Alerts", "89", "+5%")
    st.metric("Auto-Blocked", "734", "✓")
    
    st.markdown("---")
    st.markdown("### ℹ️ About")
    st.markdown("""
    **Version:** 2.0  
    **Author:** Nicolas Zalazar  
    **Stack:** Snowflake + Snowpark + Streamlit  
    
    [GitHub Repository](https://github.com/Nicolenki7)
    """)

# ============================================================================
# MOCK DATA GENERATION (For Demo - Replace with Snowflake queries in production)
# ============================================================================

@st.cache_data(ttl=300)
def load_mock_data():
    """Generate realistic mock data for demonstration."""
    np.random.seed(42)
    n_alerts = 500
    
    # Generate alerts
    data = {
        'ALERT_ID': [f"ALERT-{i:06d}" for i in range(n_alerts)],
        'IP_ADDRESS': [f"{'.'.join(map(str, np.random.randint(0, 256, 4)))}" for _ in range(n_alerts)],
        'FINAL_ACTION': np.random.choice(
            ['BLOCK_IP_IMMEDIATELY', 'SEND_TO_MANUAL_REVIEW', 'MONITOR_PASSIVELY'],
            n_alerts,
            p=[0.6, 0.3, 0.1]
        ),
        'PRIORITY': np.random.choice(['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'], n_alerts, p=[0.15, 0.35, 0.35, 0.15]),
        'RISK_SCORE': np.random.beta(2, 3, n_alerts) * 0.8 + 0.2,
        'RISK_LEVEL': np.random.choice(['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'], n_alerts, p=[0.15, 0.35, 0.35, 0.15]),
        'COUNTRY_CODE': np.random.choice(['US', 'CN', 'RU', 'BR', 'IN', 'DE', 'GB', 'FR'], n_alerts),
        'total_clicks': np.random.poisson(2000, n_alerts),
        'fastest_click_ms': np.random.exponential(50, n_alerts) + 5,
        'CONFIDENCE_LEVEL': np.random.beta(5, 2, n_alerts) * 0.3 + 0.7,
        'CREATED_AT': [datetime.now() - timedelta(hours=np.random.randint(0, 168)) for _ in range(n_alerts)],
        'ACTION_STATUS': np.random.choice(['PENDING', 'EXECUTED', 'ESCALATED', 'DISMISSED'], n_alerts, p=[0.4, 0.4, 0.1, 0.1])
    }
    
    df = pd.DataFrame(data)
    
    # Add evidence summary
    df['EVIDENCE_SUMMARY'] = df.apply(
        lambda row: f"Risk: {row['RISK_SCORE']:.2f} | {row['total_clicks']:,} clicks | {row['COUNTRY_CODE']}",
        axis=1
    )
    
    return df

# Load data
try:
    df_alerts = load_mock_data()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# ============================================================================
# PAGE: OVERVIEW
# ============================================================================

if page == "📊 Overview":
    st.header("📊 Platform Overview")
    st.markdown("Real-time monitoring of fraud detection activities")
    st.markdown("---")
    
    # Top-level metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_alerts = len(df_alerts)
        st.metric(
            label="Total Alerts",
            value=f"{total_alerts:,}",
            delta="+12% vs last 24h"
        )
    
    with col2:
        critical_alerts = len(df_alerts[df_alerts['PRIORITY'] == 'CRITICAL'])
        st.metric(
            label="Critical Alerts",
            value=f"{critical_alerts:,}",
            delta="+5%",
            delta_color="inverse"
        )
    
    with col3:
        blocked = len(df_alerts[df_alerts['FINAL_ACTION'] == 'BLOCK_IP_IMMEDIATELY'])
        st.metric(
            label="IPs Blocked",
            value=f"{blocked:,}",
            delta="Auto-executed"
        )
    
    with col4:
        pending = len(df_alerts[df_alerts['ACTION_STATUS'] == 'PENDING'])
        st.metric(
            label="Pending Review",
            value=f"{pending:,}",
            delta="Requires action",
            delta_color="inverse"
        )
    
    st.markdown("---")
    
    # Main charts
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📈 Alert Trend (Last 7 Days)")
        
        # Aggregate by date
        df_trend = df_alerts.copy()
        df_trend['DATE'] = df_trend['CREATED_AT'].dt.date
        trend_data = df_trend.groupby('DATE').size().reset_index(name='COUNT')
        
        fig_trend = px.area(
            trend_data,
            x='DATE',
            y='COUNT',
            title='Alerts Over Time',
            template='plotly_white'
        )
        fig_trend.update_traces(line_color='#667eea', fillcolor='rgba(102, 126, 234, 0.3)')
        st.plotly_chart(fig_trend, use_container_width=True)
    
    with col2:
        st.subheader("🎯 Action Distribution")
        
        action_dist = df_alerts['FINAL_ACTION'].value_counts().reset_index()
        action_dist.columns = ['Action', 'Count']
        
        fig_pie = px.pie(
            action_dist,
            values='Count',
            names='Action',
            color='Action',
            color_discrete_map={
                'BLOCK_IP_IMMEDIATELY': '#ef4444',
                'SEND_TO_MANUAL_REVIEW': '#f59e0b',
                'MONITOR_PASSIVELY': '#10b981'
            }
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    # Risk level distribution
    st.subheader("⚡ Risk Level Distribution")
    
    risk_dist = df_alerts['RISK_LEVEL'].value_counts().reset_index()
    risk_dist.columns = ['Risk Level', 'Count']
    
    fig_risk = px.bar(
        risk_dist,
        x='Risk Level',
        y='Count',
        color='Risk Level',
        color_discrete_map={
            'CRITICAL': '#dc2626',
            'HIGH': '#ea580c',
            'MEDIUM': '#ca8a04',
            'LOW': '#16a34a'
        },
        template='plotly_white'
    )
    st.plotly_chart(fig_risk, use_container_width=True)
    
    # Recent alerts table
    st.subheader("🔔 Recent Critical Alerts")
    
    critical_df = df_alerts[df_alerts['PRIORITY'] == 'CRITICAL'].head(10)
    
    st.dataframe(
        critical_df[['ALERT_ID', 'IP_ADDRESS', 'RISK_SCORE', 'FINAL_ACTION', 'COUNTRY_CODE', 'CREATED_AT']],
        use_container_width=True,
        hide_index=True
    )

# ============================================================================
# PAGE: ACTIVE ALERTS
# ============================================================================

elif page == "🚨 Active Alerts":
    st.header("🚨 Active Alerts Management")
    st.markdown("Review and manage fraud detection alerts")
    st.markdown("---")
    
    # Filter controls
    col1, col2, col3 = st.columns(3)
    
    with col1:
        filter_priority = st.selectbox(
            "Priority",
            ["All", "CRITICAL", "HIGH", "MEDIUM", "LOW"]
        )
    
    with col2:
        filter_action = st.selectbox(
            "Action",
            ["All", "BLOCK_IP_IMMEDIATELY", "SEND_TO_MANUAL_REVIEW", "MONITOR_PASSIVELY"]
        )
    
    with col3:
        filter_status = st.selectbox(
            "Status",
            ["All", "PENDING", "EXECUTED", "ESCALATED", "DISMISSED"]
        )
    
    # Apply filters
    df_filtered = df_alerts.copy()
    
    if filter_priority != "All":
        df_filtered = df_filtered[df_filtered['PRIORITY'] == filter_priority]
    
    if filter_action != "All":
        df_filtered = df_filtered[df_filtered['FINAL_ACTION'] == filter_action]
    
    if filter_status != "All":
        df_filtered = df_filtered[df_filtered['ACTION_STATUS'] == filter_status]
    
    st.markdown(f"**{len(df_filtered)} alerts** matching filters")
    
    # Alerts table
    st.dataframe(
        df_filtered[[
            'ALERT_ID', 'IP_ADDRESS', 'PRIORITY', 'RISK_SCORE',
            'FINAL_ACTION', 'ACTION_STATUS', 'COUNTRY_CODE',
            'total_clicks', 'fastest_click_ms', 'CREATED_AT'
        ]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "RISK_SCORE": st.column_config.ProgressColumn(
                "Risk Score",
                min_value=0,
                max_value=1,
                format="%.2f"
            ),
            "total_clicks": st.column_config.NumberColumn(
                "Clicks",
                format="%d"
            ),
            "fastest_click_ms": st.column_config.NumberColumn(
                "Min Click (ms)",
                format="%d"
            ),
            "CREATED_AT": st.column_config.DatetimeColumn(
                "Created",
                format="YYYY-MM-DD HH:mm"
            )
        }
    )
    
    # Action buttons
    st.markdown("### ⚡ Bulk Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("✅ Execute All Blocks", use_container_width=True):
            st.success("Block actions queued for execution")
    
    with col2:
        if st.button("📧 Escalate to Security", use_container_width=True):
            st.info("Escalation notifications sent")
    
    with col3:
        if st.button("🗑️ Dismiss Selected", use_container_width=True):
            st.warning("Selected alerts marked as dismissed")

# ============================================================================
# PAGE: GEOGRAPHIC ANALYSIS
# ============================================================================

elif page == "🌍 Geographic Analysis":
    st.header("🌍 Geographic Fraud Distribution")
    st.markdown("Analyze fraud patterns by country and region")
    st.markdown("---")
    
    # Country-level analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Fraud by Country")
        
        country_data = df_alerts.groupby('COUNTRY_CODE').size().reset_index(name='COUNT')
        country_data = country_data.sort_values('COUNT', ascending=True)
        
        fig_country = px.bar(
            country_data,
            y='COUNTRY_CODE',
            x='COUNT',
            orientation='h',
            title='Alerts by Country',
            color='COUNT',
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig_country, use_container_width=True)
    
    with col2:
        st.subheader("Risk Score by Country")
        
        country_risk = df_alerts.groupby('COUNTRY_CODE')['RISK_SCORE'].mean().reset_index()
        country_risk = country_risk.sort_values('RISK_SCORE', ascending=False)
        
        fig_risk_country = px.bar(
            country_risk,
            x='COUNTRY_CODE',
            y='RISK_SCORE',
            title='Average Risk Score by Country',
            color='RISK_SCORE',
            color_continuous_scale='YlOrRd'
        )
        st.plotly_chart(fig_risk_country, use_container_width=True)
    
    # World map (simulated coordinates)
    st.subheader("🗺️ Global Fraud Heatmap")
    
    # Add mock coordinates
    country_coords = {
        'US': (37.0902, -95.7129),
        'CN': (35.8617, 104.1954),
        'RU': (61.5240, 105.3188),
        'BR': (-14.2350, -51.9253),
        'IN': (20.5937, 78.9629),
        'DE': (51.1657, 10.4515),
        'GB': (55.3781, -3.4360),
        'FR': (46.2276, 2.2137)
    }
    
    df_geo = df_alerts.copy()
    df_geo['LAT'] = df_geo['COUNTRY_CODE'].map(lambda x: country_coords.get(x, (0, 0))[0])
    df_geo['LON'] = df_geo['COUNTRY_CODE'].map(lambda x: country_coords.get(x, (0, 0))[1])
    
    fig_map = px.scatter_geo(
        df_geo,
        lat='LAT',
        lon='LON',
        color='RISK_LEVEL',
        size='total_clicks',
        hover_name='COUNTRY_CODE',
        color_discrete_map={
            'CRITICAL': '#dc2626',
            'HIGH': '#ea580c',
            'MEDIUM': '#ca8a04',
            'LOW': '#16a34a'
        },
        projection='natural earth',
        title='Global Fraud Distribution'
    )
    st.plotly_chart(fig_map, use_container_width=True)

# ============================================================================
# PAGE: TRENDS
# ============================================================================

elif page == "📈 Trends":
    st.header("📈 Historical Trends Analysis")
    st.markdown("Analyze fraud patterns over time")
    st.markdown("---")
    
    # Time-based aggregation
    df_trends = df_alerts.copy()
    df_trends['HOUR'] = df_trends['CREATED_AT'].dt.hour
    df_trends['DAY_OF_WEEK'] = df_trends['CREATED_AT'].dt.day_name()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Alerts by Hour of Day")
        
        hourly = df_trends.groupby('HOUR').size().reset_index(name='COUNT')
        
        fig_hourly = px.line(
            hourly,
            x='HOUR',
            y='COUNT',
            title='Alert Volume by Hour',
            markers=True
        )
        st.plotly_chart(fig_hourly, use_container_width=True)
    
    with col2:
        st.subheader("Alerts by Day of Week")
        
        daily = df_trends.groupby('DAY_OF_WEEK').size().reset_index(name='COUNT')
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        daily['DAY_OF_WEEK'] = pd.Categorical(daily['DAY_OF_WEEK'], categories=day_order, ordered=True)
        daily = daily.sort_values('DAY_OF_WEEK')
        
        fig_daily = px.bar(
            daily,
            x='DAY_OF_WEEK',
            y='COUNT',
            title='Alert Volume by Day',
            color='COUNT',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig_daily, use_container_width=True)
    
    # Risk score trend
    st.subheader("Average Risk Score Over Time")
    
    df_trends['DATE'] = df_trends['CREATED_AT'].dt.date
    daily_risk = df_trends.groupby('DATE')['RISK_SCORE'].mean().reset_index()
    
    fig_risk_trend = px.line(
        daily_risk,
        x='DATE',
        y='RISK_SCORE',
        title='Average Risk Score Trend',
        markers=True
    )
    st.plotly_chart(fig_risk_trend, use_container_width=True)

# ============================================================================
# PAGE: MODEL PERFORMANCE
# ============================================================================

elif page == "⚙️ Model Performance":
    st.header("⚙️ Model Performance Metrics")
    st.markdown("Evaluate fraud detection model effectiveness")
    st.markdown("---")
    
    # Simulated confusion matrix data
    st.subheader("📊 Confusion Matrix")
    
    confusion_data = {
        'Actual \\ Predicted': ['Bot', 'Human'],
        'Bot': [89, 11],
        'Human': [3, 97]
    }
    
    fig_cm = px.imshow(
        [[89, 11], [3, 97]],
        labels=dict(x="Predicted", y="Actual", color="Count"),
        x=['Bot', 'Human'],
        y=['Bot', 'Human'],
        color_continuous_scale='Blues',
        text_auto=True
    )
    fig_cm.update_layout(title='Confusion Matrix')
    st.plotly_chart(fig_cm, use_container_width=True)
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Precision",
            value="96.7%",
            help="TP / (TP + FP)"
        )
    
    with col2:
        st.metric(
            label="Recall",
            value="89.0%",
            help="TP / (TP + FN)"
        )
    
    with col3:
        st.metric(
            label="F1 Score",
            value="92.7%",
            help="Harmonic mean of precision/recall"
        )
    
    with col4:
        st.metric(
            label="Accuracy",
            value="93.0%",
            help="(TP + TN) / Total"
        )
    
    st.markdown("---")
    
    # Business impact
    st.subheader("💰 Business Impact Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Fraud Prevention")
        st.success("**$127,450** - Estimated budget saved from blocked fraud")
        st.info("89 bots correctly identified and blocked")
    
    with col2:
        st.markdown("#### False Positive Cost")
        st.warning("**$1,850** - Estimated revenue loss from false positives")
        st.error("3 legitimate users incorrectly blocked")
    
    # ROC curve simulation
    st.subheader("📈 ROC Curve")
    
    fpr = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    tpr = [0, 0.4, 0.6, 0.75, 0.85, 0.9, 0.93, 0.95, 0.97, 0.99, 1.0]
    
    fig_roc = go.Figure()
    fig_roc.add_trace(go.Scatter(
        x=fpr,
        y=tpr,
        mode='lines+markers',
        name='Model',
        line=dict(color='#667eea', width=3)
    ))
    fig_roc.add_trace(go.Scatter(
        x=[0, 1],
        y=[0, 1],
        mode='lines',
        name='Random',
        line=dict(color='gray', dash='dash')
    ))
    fig_roc.update_layout(
        title='ROC Curve',
        xaxis_title='False Positive Rate',
        yaxis_title='True Positive Rate',
        template='plotly_white'
    )
    st.plotly_chart(fig_roc, use_container_width=True)

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #6b7280; font-size: 0.9rem;'>
        <b>AdTech Fraud Detection Platform v2.0</b> | 
        Built by Nicolas Zalazar | 
        Powered by Snowflake + Snowpark + Streamlit
    </div>
    """,
    unsafe_allow_html=True
)
