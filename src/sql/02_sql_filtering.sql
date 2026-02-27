-- ============================================================================
-- AdTech Fraud Detection Platform - High-Volume & High-Velocity Filtering
-- ============================================================================
-- Purpose: Multi-stage SQL filtering to reduce 10TB+ to <1% suspect IPs
-- Author: Nicolas Zalazar
-- Version: 2.0 (Unified Platform)
-- Date: 2026-02-28
-- ============================================================================

-- ----------------------------------------------------------------------------
-- CONTEXT SETUP
-- ----------------------------------------------------------------------------

USE ROLE SYSADMIN;
USE WAREHOUSE FRAUD_ANALYSIS_WH;
USE DATABASE FRAUD_DETECTION_DB;
USE SCHEMA RAW_LOGS;

-- ----------------------------------------------------------------------------
-- PHASE 1: HIGH-VOLUME FILTERING (COUNT/HAVING)
-- ----------------------------------------------------------------------------
-- Detects IPs with abnormally high click counts or UA diversity
-- Reduces dataset by ~95%
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE SUSPECT_IPS_HIGH_VOLUME AS
WITH volume_metrics AS (
    SELECT 
        IP_ADDRESS,
        COUNT(*) as total_events,
        COUNT(DISTINCT USER_AGENT) as distinct_uas,
        COUNT(DISTINCT CAMPAIGN_ID) as distinct_campaigns,
        COUNT(DISTINCT PUBLISHER_ID) as distinct_publishers,
        MIN(TIMESTAMP) as first_seen,
        MAX(TIMESTAMP) as last_seen,
        ANY_VALUE(COUNTRY_CODE) as country_code,
        ANY_VALUE(DEVICE_TYPE) as device_type
    FROM BID_LOGS
    WHERE TIMESTAMP >= DATEADD(hour, -24, CURRENT_TIMESTAMP())  -- Last 24 hours
    GROUP BY IP_ADDRESS
)
SELECT 
    IP_ADDRESS,
    total_events,
    distinct_uas,
    distinct_campaigns,
    distinct_publishers,
    first_seen,
    last_seen,
    country_code,
    device_type,
    -- Fraud flags array
    ARRAY_CONSTRUCT_COMPACT(
        CASE WHEN total_events > 1000 THEN 'HIGH_VOLUME' END,
        CASE WHEN distinct_uas > 5 THEN 'UA_ROTATION' END,
        CASE WHEN distinct_campaigns > 50 THEN 'CAMPAIGN_SPAM' END,
        CASE WHEN distinct_publishers > 20 THEN 'PUBLISHER_HOPPING' END
    ) as fraud_flags,
    -- Primary fraud reason
    CASE 
        WHEN total_events > 5000 THEN 'EXTREME_HIGH_VOLUME'
        WHEN total_events > 1000 THEN 'HIGH_VOLUME_SUSPECT'
        WHEN distinct_uas > 10 THEN 'AGGRESSIVE_UA_ROTATION'
        WHEN distinct_uas > 5 THEN 'UA_ROTATION_BOT'
        WHEN distinct_campaigns > 50 THEN 'CAMPAIGN_SPAMMER'
        ELSE 'VOLUME_ANOMALY'
    END as fraud_reason
FROM volume_metrics
WHERE 
    total_events > 1000           -- Threshold: 1000+ events
    OR distinct_uas > 5           -- Threshold: 5+ different user agents
    OR distinct_campaigns > 50    -- Threshold: 50+ campaigns
ORDER BY total_events DESC;

-- Log results
SELECT 
    'HIGH_VOLUME_FILTER' as FILTER_PHASE,
    COUNT(*) as SUSPECT_COUNT,
    SUM(total_events) as TOTAL_EVENTS,
    AVG(total_events) as AVG_EVENTS_PER_IP,
    COUNT(DISTINCT fraud_reason) as FRAUD_TYPES
FROM SUSPECT_IPS_HIGH_VOLUME;

-- ----------------------------------------------------------------------------
-- PHASE 2: HIGH-VELOCITY FILTERING (Window Functions)
-- ----------------------------------------------------------------------------
-- Detects IPs with bot-like click speed patterns
-- Reduces dataset by additional ~80%
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE SUSPECT_IPS_HIGH_VELOCITY AS
WITH velocity_metrics AS (
    SELECT 
        IP_ADDRESS,
        TIMESTAMP,
        USER_AGENT,
        TIME_TO_CLICK_MS,
        COUNTRY_CODE,
        DEVICE_TYPE,
        -- Calculate time difference from previous click (same IP)
        LAG(TIMESTAMP) OVER (
            PARTITION BY IP_ADDRESS 
            ORDER BY TIMESTAMP
        ) as prev_timestamp,
        -- Calculate click velocity
        TIMESTAMPDIFF(
            MILLISECOND, 
            LAG(TIMESTAMP) OVER (PARTITION BY IP_ADDRESS ORDER BY TIMESTAMP),
            TIMESTAMP
        ) as time_diff_ms
    FROM BID_LOGS
    WHERE TIMESTAMP >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
),
velocity_aggregates AS (
    SELECT 
        IP_ADDRESS,
        COUNT(*) as total_clicks,
        MIN(time_diff_ms) as fastest_click_ms,
        AVG(time_diff_ms) as avg_click_interval_ms,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_diff_ms) as median_click_ms,
        STDDEV(time_diff_ms) as click_interval_stddev,
        COUNT(CASE WHEN time_diff_ms < 50 THEN 1 END) as ultra_fast_clicks,  -- <50ms
        COUNT(CASE WHEN time_diff_ms < 100 THEN 1 END) as very_fast_clicks,  -- <100ms
        ANY_VALUE(COUNTRY_CODE) as country_code,
        ANY_VALUE(DEVICE_TYPE) as device_type,
        MIN(TIMESTAMP) as first_seen,
        MAX(TIMESTAMP) as last_seen
    FROM velocity_metrics
    WHERE time_diff_ms IS NOT NULL  -- Exclude first click (no previous)
    GROUP BY IP_ADDRESS
)
SELECT 
    IP_ADDRESS,
    total_clicks,
    fastest_click_ms,
    avg_click_interval_ms,
    median_click_ms,
    click_interval_stddev,
    ultra_fast_clicks,
    very_fast_clicks,
    country_code,
    device_type,
    first_seen,
    last_seen,
    -- Fraud flags
    ARRAY_CONSTRUCT_COMPACT(
        CASE WHEN fastest_click_ms < 10 THEN 'INHUMAN_SPEED' END,      -- <10ms = impossible for humans
        CASE WHEN fastest_click_ms < 50 THEN 'BOT_LIKE_SPEED' END,     -- <50ms = highly suspicious
        CASE WHEN fastest_click_ms < 100 THEN 'SUSPICIOUS_SPEED' END,  -- <100ms = suspicious
        CASE WHEN ultra_fast_clicks > 100 THEN 'REPEATED_ULTRA_FAST' END,
        CASE WHEN avg_click_interval_ms < 200 THEN 'CONSISTENTLY_FAST' END
    ) as fraud_flags,
    -- Primary fraud reason
    CASE 
        WHEN fastest_click_ms < 10 THEN 'INHUMAN_VELOCITY_BOT'
        WHEN fastest_click_ms < 50 THEN 'HIGH_VELOCITY_BOT'
        WHEN fastest_click_ms < 100 THEN 'SUSPICIOUS_VELOCITY'
        WHEN ultra_fast_clicks > 100 THEN 'REPEATED_FAST_PATTERN'
        ELSE 'VELOCITY_ANOMALY'
    END as fraud_reason
FROM velocity_aggregates
WHERE 
    fastest_click_ms < 100          -- Threshold: <100ms minimum
    OR ultra_fast_clicks > 100      -- Threshold: 100+ ultra-fast clicks
    OR avg_click_interval_ms < 200  -- Threshold: <200ms average
ORDER BY fastest_click_ms ASC;

-- Log results
SELECT 
    'HIGH_VELOCITY_FILTER' as FILTER_PHASE,
    COUNT(*) as SUSPECT_COUNT,
    AVG(fastest_click_ms) as AVG_FASTEST_CLICK,
    MIN(fastest_click_ms) as MIN_FASTEST_CLICK,
    SUM(ultra_fast_clicks) as TOTAL_ULTRA_FAST_CLICKS
FROM SUSPECT_IPS_HIGH_VELOCITY;

-- ----------------------------------------------------------------------------
-- PHASE 3: MERGE RESULTS (Unified Suspect Table)
-- ----------------------------------------------------------------------------
-- Combines both filtering phases with deduplication
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE SUSPECT_IPS AS
WITH combined_suspects AS (
    -- High volume suspects
    SELECT 
        IP_ADDRESS,
        total_events as total_clicks,
        distinct_uas,
        NULL as fastest_click_ms,
        NULL as avg_click_interval_ms,
        fraud_flags,
        fraud_reason,
        country_code,
        device_type,
        first_seen,
        last_seen,
        'HIGH_VOLUME' as detection_method
    FROM SUSPECT_IPS_HIGH_VOLUME
    
    UNION
    
    -- High velocity suspects
    SELECT 
        IP_ADDRESS,
        total_clicks,
        NULL as distinct_uas,
        fastest_click_ms,
        avg_click_interval_ms,
        fraud_flags,
        fraud_reason,
        country_code,
        device_type,
        first_seen,
        last_seen,
        'HIGH_VELOCITY' as detection_method
    FROM SUSPECT_IPS_HIGH_VELOCITY
),
aggregated AS (
    SELECT 
        IP_ADDRESS,
        SUM(total_clicks) as total_clicks,
        MAX(distinct_uas) as distinct_uas,
        MIN(fastest_click_ms) as fastest_click_ms,
        AVG(avg_click_interval_ms) as avg_click_interval_ms,
        ARRAY_CAT(ARRAY_AGG(fraud_flags)) as all_fraud_flags,
        ARRAY_AGG(DISTINCT fraud_reason) as fraud_reasons,
        ARRAY_AGG(DISTINCT detection_method) as detection_methods,
        ANY_VALUE(country_code) as country_code,
        ANY_VALUE(device_type) as device_type,
        MIN(first_seen) as first_seen,
        MAX(last_seen) as last_seen
    FROM combined_suspects
    GROUP BY IP_ADDRESS
)
SELECT 
    IP_ADDRESS,
    total_clicks,
    distinct_uas,
    fastest_click_ms,
    avg_click_interval_ms,
    ARRAY_SLICE(ARRAY_DISTINCT(FLATTEN(all_fraud_flags)), 0, 10) as fraud_flags,  -- Dedupe and limit
    fraud_reasons,
    detection_methods,
    country_code,
    device_type,
    first_seen,
    last_seen,
    -- Primary fraud reason (highest priority)
    CASE 
        WHEN ARRAY_CONTAINS('INHUMAN_VELOCITY_BOT'::VARIANT, fraud_reasons) THEN 'INHUMAN_VELOCITY_BOT'
        WHEN ARRAY_CONTAINS('HIGH_VELOCITY_BOT'::VARIANT, fraud_reasons) THEN 'HIGH_VELOCITY_BOT'
        WHEN ARRAY_CONTAINS('EXTREME_HIGH_VOLUME'::VARIANT, fraud_reasons) THEN 'EXTREME_HIGH_VOLUME'
        WHEN ARRAY_CONTAINS('HIGH_VOLUME_SUSPECT'::VARIANT, fraud_reasons) THEN 'HIGH_VOLUME_SUSPECT'
        WHEN ARRAY_CONTAINS('UA_ROTATION_BOT'::VARIANT, fraud_reasons) THEN 'UA_ROTATION_BOT'
        ELSE fraud_reasons[0]::VARCHAR
    END as primary_fraud_reason,
    CURRENT_TIMESTAMP() as detected_at
FROM aggregated
ORDER BY total_clicks DESC;

-- ----------------------------------------------------------------------------
-- FINAL SUMMARY
-- ----------------------------------------------------------------------------

SELECT 
    'FILTERING_COMPLETE' as STATUS,
    COUNT(*) as TOTAL_SUSPECTS,
    COUNT(DISTINCT primary_fraud_reason) as FRAUD_TYPES_DETECTED,
    SUM(total_clicks) as TOTAL_SUSPICIOUS_CLICKS,
    AVG(total_clicks) as AVG_CLICKS_PER_SUSPECT,
    COUNT(DISTINCT country_code) as AFFECTED_COUNTRIES,
    MIN(detected_at) as DETECTION_TIMESTAMP
FROM SUSPECT_IPS;

-- Sample output for verification
SELECT TOP 20 * FROM SUSPECT_IPS ORDER BY total_clicks DESC;

-- ============================================================================
-- FILTERING COMPLETE - Ready for Snowpark Feature Engineering
-- ============================================================================
-- Next: Execute src/python/feature_engineering.py for risk scoring
-- ============================================================================
