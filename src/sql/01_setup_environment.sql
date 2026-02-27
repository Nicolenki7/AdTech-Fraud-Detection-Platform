-- ============================================================================
-- AdTech Fraud Detection Platform - Environment Setup
-- ============================================================================
-- Purpose: Initialize database, schema, warehouse, and base tables
-- Author: Nicolas Zalazar
-- Version: 2.0 (Unified Platform)
-- Date: 2026-02-28
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: RESOURCE CREATION (Warehouse & Database)
-- ----------------------------------------------------------------------------

USE ROLE SYSADMIN;

-- Create compute warehouse with cost optimization settings
CREATE WAREHOUSE IF NOT EXISTS FRAUD_ANALYSIS_WH
WITH
    WAREHOUSE_SIZE = 'XSMALL'           -- Cost-efficient for filtering workloads
    AUTO_SUSPEND = 60                   -- Suspend after 60 seconds of inactivity
    AUTO_RESUME = TRUE                  -- Auto-resume on query arrival
    INITIALLY_SUSPENDED = FALSE         -- Start ready for use
    COMMENT = 'Warehouse for AdTech Fraud Detection Pipeline';

-- Create project database
CREATE DATABASE IF NOT EXISTS FRAUD_DETECTION_DB
COMMENT = 'AdTech Fraud Detection Platform - Main Database';

-- Set context to new database
USE DATABASE FRAUD_DETECTION_DB;

-- ----------------------------------------------------------------------------
-- STEP 2: SCHEMA CREATION
-- ----------------------------------------------------------------------------

-- Raw data schema (ingestion layer)
CREATE SCHEMA IF NOT EXISTS RAW_LOGS
COMMENT = 'Raw and processed log data for fraud detection';

-- ML schema (model evaluation and ground truth)
CREATE SCHEMA IF NOT EXISTS ML_EVALUATION
COMMENT = 'Machine learning model evaluation and validation';

-- Set working schema
USE SCHEMA RAW_LOGS;

-- ----------------------------------------------------------------------------
-- STEP 3: TABLE CREATION - BID_LOGS (Raw Data - 10TB+ Scale)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE BID_LOGS (
    -- Event Identification
    BID_REQUEST_ID      VARCHAR(100)    NOT NULL,   -- Unique bid request identifier
    TIMESTAMP           TIMESTAMP_NTZ   NOT NULL,   -- Event timestamp (UTC)
    
    -- Traffic Source
    IP_ADDRESS          VARCHAR(45)     NOT NULL,   -- IPv4/IPv6 address
    USER_AGENT          VARCHAR(1000),              -- Browser/device user agent string
    
    -- Click Behavior
    TIME_TO_CLICK_MS    INTEGER,                    -- Time from impression to click (milliseconds)
    
    -- Campaign Context
    CAMPAIGN_ID         VARCHAR(50),                -- Advertising campaign identifier
    PUBLISHER_ID        VARCHAR(50),                -- Publisher/website identifier
    ADVERTISER_ID       VARCHAR(50),                -- Advertiser identifier
    
    -- Device & Location
    DEVICE_TYPE         VARCHAR(20),                -- mobile, desktop, tablet
    COUNTRY_CODE        VARCHAR(2),                 -- ISO 2-letter country code
    REGION              VARCHAR(100),               -- State/region
    
    -- Metadata
    EVENT_TYPE          VARCHAR(20)     DEFAULT 'click',  -- click, impression, conversion
    PROCESSED_AT        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw bid log data - simulated 10TB+ scale for fraud detection';

-- ----------------------------------------------------------------------------
-- STEP 4: TABLE CREATION - SUSPECT_IPS (Filtered Results)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE SUSPECT_IPS (
    -- Identification
    IP_ADDRESS          VARCHAR(45)     NOT NULL,
    
    -- Volume Metrics
    TOTAL_EVENTS        INTEGER         NOT NULL,   -- Total click/impression count
    DISTINCT_UAS        INTEGER,                    -- Unique user agents seen
    
    -- Velocity Metrics
    FASTEST_CLICK_MS    INTEGER,                    -- Minimum time between clicks
    AVG_CLICK_MS        FLOAT,                      -- Average time between clicks
    
    -- Fraud Indicators
    FRAUD_REASON        VARCHAR(50),                -- HIGH_VELOCITY_BOT, UA_ROTATION_BOT, etc.
    FRAUD_FLAGS         ARRAY,                      -- Array of all detected fraud indicators
    
    -- Enrichment
    COUNTRY_CODE        VARCHAR(2),                 -- Geo-IP country
    FIRST_SEEN          TIMESTAMP_NTZ,              -- First occurrence timestamp
    LAST_SEEN           TIMESTAMP_NTZ,              -- Most recent occurrence
    
    -- Processing Metadata
    DETECTED_AT         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Suspect IPs after SQL filtering phase (<1% of original data)';

-- ----------------------------------------------------------------------------
-- STEP 5: TABLE CREATION - FINAL_RISK_SCORES (Snowpark Output)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE FINAL_RISK_SCORES (
    -- Core Identification
    IP_ADDRESS          VARCHAR(45)     NOT NULL,
    
    -- Risk Assessment
    RISK_SCORE          FLOAT           NOT NULL,   -- 0.0 - 1.0 risk score
    RISK_LEVEL          VARCHAR(20),                -- LOW, MEDIUM, HIGH, CRITICAL
    
    -- Feature Scores (for explainability)
    VELOCITY_SCORE      FLOAT,                      -- Component score from velocity
    VOLUME_SCORE        FLOAT,                      -- Component score from volume
    UA_ROTATION_SCORE   FLOAT,                      -- Component score from UA diversity
    
    -- Classification
    FRAUD_FLAG          BOOLEAN,                    -- Binary fraud flag
    CONFIDENCE_LEVEL    FLOAT,                      -- Model confidence (0-1)
    
    -- Enrichment
    COUNTRY_CODE        VARCHAR(2),
    DEVICE_TYPE         VARCHAR(20),
    
    -- Metadata
    SCORED_AT           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Risk-scored suspects after Snowpark feature engineering';

-- ----------------------------------------------------------------------------
-- STEP 6: TABLE CREATION - ACTIONABLE_ALERTS (Final Output)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE ACTIONABLE_ALERTS (
    -- Alert Identification
    ALERT_ID            VARCHAR(100)    DEFAULT UUID_STRING(),
    IP_ADDRESS          VARCHAR(45)     NOT NULL,
    
    -- Decision
    FINAL_ACTION        VARCHAR(30)     NOT NULL,   -- BLOCK_IP_IMMEDIATELY, SEND_TO_MANUAL_REVIEW, MONITOR_PASSIVELY
    PRIORITY            VARCHAR(10),                -- CRITICAL, HIGH, MEDIUM, LOW
    
    -- Supporting Evidence
    RISK_SCORE          FLOAT,
    FRAUD_REASON        VARCHAR(100),
    EVIDENCE_SUMMARY    VARCHAR(500),               -- Human-readable summary
    
    -- Action Tracking
    ACTION_STATUS       VARCHAR(20)     DEFAULT 'PENDING',  -- PENDING, EXECUTED, ESCALATED, DISMISSED
    ACTIONED_BY         VARCHAR(100),               -- System or human analyst
    ACTIONED_AT         TIMESTAMP_NTZ,
    
    -- Metadata
    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    EXPIRES_AT          TIMESTAMP_NTZ               -- Alert expiration (for auto-cleanup)
)
COMMENT = 'Final actionable alerts with BLOCK/REVIEW/MONITOR decisions';

-- ----------------------------------------------------------------------------
-- STEP 7: TABLE CREATION - MODEL_PERFORMANCE_ANALYSIS (Evaluation)
-- ----------------------------------------------------------------------------

USE SCHEMA ML_EVALUATION;

CREATE OR REPLACE TABLE TRAFFIC_EVALUATION (
    IP_ADDRESS          VARCHAR(45)     NOT NULL,
    TOTAL_CLICKS        INTEGER         NOT NULL,
    IS_ACTUALLY_BOT     BOOLEAN         NOT NULL,   -- Ground truth label
    USER_TYPE           VARCHAR(50),                -- Script_Bot, Normal_User, Gamer_User, etc.
    REVENUE_IMPACT      FLOAT,                      -- Estimated revenue impact if blocked
    EVALUATION_DATE     DATE          DEFAULT CURRENT_DATE()
)
COMMENT = 'Ground truth labeled data for model evaluation';

CREATE OR REPLACE TABLE MODEL_PERFORMANCE_ANALYSIS (
    IP_ADDRESS          VARCHAR(45)     NOT NULL,
    TOTAL_CLICKS        INTEGER,
    USER_TYPE           VARCHAR(50),
    IS_ACTUALLY_BOT     BOOLEAN,
    
    -- Model Prediction
    PREDICTED_AS_BOT    BOOLEAN,
    
    -- Confusion Matrix Classification
    EVALUATION_CATEGORY VARCHAR(30),    -- TRUE_POSITIVE, TRUE_NEGATIVE, FALSE_POSITIVE, FALSE_NEGATIVE
    
    -- Business Impact
    BUSINESS_IMPACT     VARCHAR(100),
    REVENUE_IMPACT      FLOAT,
    
    ANALYSIS_DATE       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Model performance analysis with confusion matrix and business impact';

-- ----------------------------------------------------------------------------
-- STEP 8: VERIFICATION
-- ----------------------------------------------------------------------------

USE ROLE SYSADMIN;

-- Verify all tables created
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    COMMENT
FROM FRAUD_DETECTION_DB.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('RAW_LOGS', 'ML_EVALUATION')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- Display current context
SELECT 
    CURRENT_ROLE() as CURRENT_ROLE,
    CURRENT_WAREHOUSE() as CURRENT_WAREHOUSE,
    CURRENT_DATABASE() as CURRENT_DATABASE,
    CURRENT_SCHEMA() as CURRENT_SCHEMA;

-- ============================================================================
-- SETUP COMPLETE
-- ============================================================================
-- Next Steps:
-- 1. Load data into BID_LOGS (or use sample data generator)
-- 2. Execute 03_high_volume_filter.sql
-- 3. Execute 04_high_velocity_filter.sql
-- 4. Run Snowpark feature engineering (src/python/feature_engineering.py)
-- 5. Deploy UDF (src/python/udf_deployment.py)
-- 6. Launch dashboard (dashboard/app.py)
-- ============================================================================
