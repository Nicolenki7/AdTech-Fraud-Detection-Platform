# Architecture Documentation

## System Overview

The AdTech Fraud Detection Platform is a multi-stage, scalable fraud detection system designed to process 10+ terabytes of bid log data and identify fraudulent traffic patterns with high precision.

## Architecture Layers

### 1. Data Ingestion Layer
- **Source**: AdTech bid logs (impressions, clicks, conversions)
- **Volume**: 10TB+ daily
- **Format**: Structured JSON/Parquet
- **Ingestion**: Snowpipe (real-time) or batch loading

### 2. Storage Layer (Snowflake)
```
FRAUD_DETECTION_DB/
├── RAW_LOGS/
│   ├── BID_LOGS (10TB+)           -- Raw event data
│   ├── SUSPECT_IPS (<1%)          -- Filtered suspects
│   ├── FINAL_RISK_SCORES          -- Scored suspects
│   └── ACTIONABLE_ALERTS          -- Final decisions
└── ML_EVALUATION/
    ├── TRAFFIC_EVALUATION         -- Ground truth labels
    └── MODEL_PERFORMANCE_ANALYSIS -- Confusion matrix
```

### 3. Processing Layer

#### Stage 1: SQL Filtering (99.9% Reduction)
- **High-Volume Detection**: COUNT/HAVING aggregations
- **High-Velocity Detection**: LAG window functions
- **Output**: <1% of original data

#### Stage 2: Snowpark Feature Engineering
- **Risk Scoring**: Multi-factor weighted algorithm
- **Features**: Velocity, volume, UA rotation, geo-risk
- **Output**: FINAL_RISK_SCORES table

#### Stage 3: UDF Classification
- **Logic**: Python UDF for BLOCK/REVIEW/MONITOR decisions
- **Priority**: CRITICAL, HIGH, MEDIUM, LOW
- **Output**: ACTIONABLE_ALERTS table

### 4. Presentation Layer
- **Streamlit Dashboard**: Real-time monitoring
- **Views**: Overview, Alerts, Geographic, Trends, Model Performance

## Data Flow

```
[BID_LOGS 10TB+]
       ↓
[SQL Filtering] ────→ 99.9% reduction
       ↓
[SUSPECT_IPS <10GB]
       ↓
[Snowpark Scoring]
       ↓
[FINAL_RISK_SCORES]
       ↓
[UDF Classification]
       ↓
[ACTIONABLE_ALERTS]
       ↓
[Streamlit Dashboard]
```

## Security Considerations

1. **Access Control**: Role-based access (SYSADMIN, ANALYST, VIEWER)
2. **Data Masking**: IP addresses masked in non-production environments
3. **Audit Logging**: All actions logged to Snowflake ACCOUNT_USAGE
4. **Network Policies**: IP allowlisting for Snowflake access

## Cost Optimization

1. **Multi-Stage Filtering**: Reduce compute by filtering early
2. **Warehouse Sizing**: X-SMALL for filtering, SMALL for scoring
3. **Auto-Suspend**: 60-second idle timeout
4. **Data Clustering**: Cluster on IP_ADDRESS and TIMESTAMP

## Scalability

- **Horizontal**: Snowflake auto-scales compute
- **Vertical**: Warehouse sizing adjusts to workload
- **Data Partitioning**: Clustered by date for efficient pruning

## Monitoring & Alerting

1. **Pipeline Health**: Monitor job success/failure rates
2. **Data Quality**: Validate row counts, null percentages
3. **Model Drift**: Track precision/recall over time
4. **Cost Monitoring**: Daily credit consumption alerts

## Disaster Recovery

1. **Backup**: Snowflake Time Travel (90 days)
2. **Failover**: Multi-region deployment available
3. **RPO**: <1 hour (via Snowpipe streaming)
4. **RTO**: <4 hours (full platform restoration)
