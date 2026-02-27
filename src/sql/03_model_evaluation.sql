-- ============================================================================
-- AdTech Fraud Detection Platform - Model Evaluation & Precision Analysis
-- ============================================================================
-- Purpose: Calculate confusion matrix, precision/recall, and business impact
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
USE SCHEMA ML_EVALUATION;

-- ----------------------------------------------------------------------------
-- STEP 1: GENERATE GROUND TRUTH DATA (If not exists)
-- ----------------------------------------------------------------------------
-- Simulates labeled data for model validation
-- In production, this comes from manual review or feedback loops
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE TRAFFIC_EVALUATION AS
SELECT 
    IP_ADDRESS,
    TOTAL_CLICKS,
    IS_ACTUALLY_BOT,
    USER_TYPE,
    -- Estimated revenue impact (for business analysis)
    CASE 
        WHEN IS_ACTUALLY_BOT = TRUE THEN 0  -- Bots generate no real revenue
        WHEN USER_TYPE = 'Gamer_User' THEN 150.00  -- High-value users
        WHEN USER_TYPE = 'Heavy_Shopper' THEN 200.00
        WHEN USER_TYPE = 'Family_Shared_IP' THEN 75.00
        WHEN USER_TYPE = 'Normal_User' THEN 50.00
        ELSE 25.00
    END as REVENUE_IMPACT
FROM (
    -- Script Bots (high volume, obvious fraud)
    SELECT '10.1.1.1' as IP_ADDRESS, 150 as TOTAL_CLICKS, TRUE as IS_ACTUALLY_BOT, 'Script_Bot' as USER_TYPE
    UNION ALL SELECT '10.1.1.2', 200, TRUE, 'Script_Bot'
    UNION ALL SELECT '10.1.1.3', 180, TRUE, 'Script_Bot'
    UNION ALL SELECT '10.1.1.4', 160, TRUE, 'Script_Bot'
    UNION ALL SELECT '10.1.1.5', 140, TRUE, 'Script_Bot'
    
    -- Normal Users (low volume, legitimate)
    UNION ALL SELECT '192.168.1.1', 5, FALSE, 'Normal_User'
    UNION ALL SELECT '192.168.1.2', 12, FALSE, 'Normal_User'
    UNION ALL SELECT '192.168.1.3', 25, FALSE, 'Normal_User'
    UNION ALL SELECT '192.168.1.4', 8, FALSE, 'Normal_User'
    UNION ALL SELECT '192.168.1.5', 15, FALSE, 'Normal_User'
    UNION ALL SELECT '192.168.1.6', 20, FALSE, 'Normal_User'
    
    -- Power Users (high volume but legitimate - FALSE POSITIVE RISK)
    UNION ALL SELECT '172.16.0.1', 55, FALSE, 'Gamer_User'
    UNION ALL SELECT '172.16.0.2', 62, FALSE, 'Heavy_Shopper'
    UNION ALL SELECT '172.16.0.3', 51, FALSE, 'Family_Shared_IP'
    UNION ALL SELECT '172.16.0.4', 70, FALSE, 'Gamer_User'
    UNION ALL SELECT '172.16.0.5', 80, FALSE, 'Heavy_Shopper'
    
    -- Low-and-Slow Bots (sophisticated - FALSE NEGATIVE RISK)
    UNION ALL SELECT '10.9.9.1', 45, TRUE, 'Low_and_Slow_Bot'
    UNION ALL SELECT '10.9.9.2', 48, TRUE, 'Mimic_Bot'
    UNION ALL SELECT '10.9.9.3', 49, TRUE, 'AI_Crawler'
    UNION ALL SELECT '10.9.9.4', 47, TRUE, 'Low_and_Slow_Bot'
    UNION ALL SELECT '10.9.9.5', 46, TRUE, 'Stealth_Bot'
) as ground_truth;

-- Verify ground truth data
SELECT 
    USER_TYPE,
    COUNT(*) as IP_COUNT,
    SUM(TOTAL_CLICKS) as TOTAL_CLICKS,
    SUM(IS_ACTUALLY_BOT::INT) as BOT_COUNT
FROM TRAFFIC_EVALUATION
GROUP BY USER_TYPE
ORDER BY BOT_COUNT DESC, IP_COUNT DESC;

-- ----------------------------------------------------------------------------
-- STEP 2: APPLY DETECTION MODEL (Heuristic Rule)
-- ----------------------------------------------------------------------------
-- Standard rule: Block if TOTAL_CLICKS > 50
-- This is our "model prediction"
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE MODEL_PERFORMANCE_ANALYSIS AS
SELECT
    e.IP_ADDRESS,
    e.TOTAL_CLICKS,
    e.USER_TYPE,
    e.IS_ACTUALLY_BOT,
    e.REVENUE_IMPACT,
    
    -- MODEL PREDICTION (Heuristic Rule)
    CASE 
        WHEN e.TOTAL_CLICKS > 50 THEN TRUE 
        ELSE FALSE 
    END as PREDICTED_AS_BOT,
    
    -- CONFUSION MATRIX CLASSIFICATION
    CASE
        -- TRUE POSITIVE: Correctly identified bot
        WHEN (e.TOTAL_CLICKS > 50) AND (e.IS_ACTUALLY_BOT = TRUE) 
            THEN 'TRUE_POSITIVE'
        
        -- TRUE NEGATIVE: Correctly identified human
        WHEN (e.TOTAL_CLICKS <= 50) AND (e.IS_ACTUALLY_BOT = FALSE) 
            THEN 'TRUE_NEGATIVE'
        
        -- FALSE POSITIVE: Incorrectly blocked human (REVENUE LOSS)
        WHEN (e.TOTAL_CLICKS > 50) AND (e.IS_ACTUALLY_BOT = FALSE) 
            THEN 'FALSE_POSITIVE'
        
        -- FALSE NEGATIVE: Missed bot (BUDGET WASTE)
        WHEN (e.TOTAL_CLICKS <= 50) AND (e.IS_ACTUALLY_BOT = TRUE) 
            THEN 'FALSE_NEGATIVE'
    END as EVALUATION_CATEGORY,
    
    -- BUSINESS IMPACT DESCRIPTION
    CASE 
        WHEN (e.TOTAL_CLICKS > 50) AND (e.IS_ACTUALLY_BOT = TRUE) 
            THEN 'Fraud Prevention - Budget Saved'
        WHEN (e.TOTAL_CLICKS <= 50) AND (e.IS_ACTUALLY_BOT = FALSE) 
            THEN 'Clean Traffic - Valid Impressions'
        WHEN (e.TOTAL_CLICKS > 50) AND (e.IS_ACTUALLY_BOT = FALSE) 
            THEN 'Lost Revenue - Angry Users/Churn'
        WHEN (e.TOTAL_CLICKS <= 50) AND (e.IS_ACTUALLY_BOT = TRUE) 
            THEN 'Fraud Leakage - Wasted Ad Budget'
    END as BUSINESS_IMPACT,
    
    CURRENT_TIMESTAMP() as ANALYSIS_DATE
FROM TRAFFIC_EVALUATION e;

-- ----------------------------------------------------------------------------
-- STEP 3: AGGREGATE PERFORMANCE METRICS
-- ----------------------------------------------------------------------------

-- Confusion Matrix Summary
SELECT 
    EVALUATION_CATEGORY,
    COUNT(*) as IP_COUNT,
    SUM(REVENUE_IMPACT) as TOTAL_REVENUE_IMPACT,
    BUSINESS_IMPACT,
    -- Percentage of total
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as PERCENTAGE
FROM MODEL_PERFORMANCE_ANALYSIS
GROUP BY EVALUATION_CATEGORY, BUSINESS_IMPACT
ORDER BY 
    CASE EVALUATION_CATEGORY
        WHEN 'TRUE_POSITIVE' THEN 1
        WHEN 'TRUE_NEGATIVE' THEN 2
        WHEN 'FALSE_POSITIVE' THEN 3
        WHEN 'FALSE_NEGATIVE' THEN 4
    END;

-- ----------------------------------------------------------------------------
-- STEP 4: CALCULATE KEY METRICS (Precision, Recall, F1)
-- ----------------------------------------------------------------------------

WITH metrics AS (
    SELECT
        SUM(CASE WHEN EVALUATION_CATEGORY = 'TRUE_POSITIVE' THEN 1 ELSE 0 END) as TP,
        SUM(CASE WHEN EVALUATION_CATEGORY = 'TRUE_NEGATIVE' THEN 1 ELSE 0 END) as TN,
        SUM(CASE WHEN EVALUATION_CATEGORY = 'FALSE_POSITIVE' THEN 1 ELSE 0 END) as FP,
        SUM(CASE WHEN EVALUATION_CATEGORY = 'FALSE_NEGATIVE' THEN 1 ELSE 0 END) as FN,
        SUM(CASE WHEN EVALUATION_CATEGORY = 'FALSE_POSITIVE' THEN REVENUE_IMPACT ELSE 0 END) as FP_REVENUE_LOSS,
        SUM(CASE WHEN EVALUATION_CATEGORY = 'FALSE_NEGATIVE' THEN 1 ELSE 0 END) as FN_BOT_COUNT
    FROM MODEL_PERFORMANCE_ANALYSIS
)
SELECT
    -- Basic Counts
    TP as TRUE_POSITIVES,
    TN as TRUE_NEGATIVES,
    FP as FALSE_POSITIVES,
    FN as FALSE_NEGATIVES,
    
    -- Precision: Of all predicted bots, how many were actually bots?
    -- High precision = few false positives (don't block humans)
    ROUND(TP * 100.0 / NULLIF(TP + FP, 0), 2) as PRECISION_PERCENT,
    
    -- Recall (Sensitivity): Of all actual bots, how many did we catch?
    -- High recall = few false negatives (catch most bots)
    ROUND(TP * 100.0 / NULLIF(TP + FN, 0), 2) as RECALL_PERCENT,
    
    -- Specificity: Of all humans, how many did we correctly allow?
    ROUND(TN * 100.0 / NULLIF(TN + FP, 0), 2) as SPECIFICITY_PERCENT,
    
    -- F1-Score: Harmonic mean of precision and recall
    ROUND(
        2 * (TP * 100.0 / NULLIF(TP + FP, 0)) * (TP * 100.0 / NULLIF(TP + FN, 0)) /
        NULLIF((TP * 100.0 / NULLIF(TP + FP, 0)) + (TP * 100.0 / NULLIF(TP + FN, 0)), 0)
    , 2) as F1_SCORE,
    
    -- Accuracy: Overall correctness
    ROUND((TP + TN) * 100.0 / NULLIF(TP + TN + FP + FN, 0), 2) as ACCURACY_PERCENT,
    
    -- Business Impact Metrics
    FP as USERS_INCORRECTLY_BLOCKED,
    FP_REVENUE_LOSS as ESTIMATED_REVENUE_LOSS_USD,
    FN_BOT_COUNT as BOTS_MISSED,
    
    -- Cost-Benefit Analysis
    TP as BOTS_CORRECTLY_BLOCKED,
    TN as HUMANS_CORRECTLY_ALLOWED
    
FROM metrics;

-- ----------------------------------------------------------------------------
-- STEP 5: ANALYZE BY USER TYPE (Root Cause Analysis)
-- ----------------------------------------------------------------------------

SELECT 
    USER_TYPE,
    COUNT(*) as IP_COUNT,
    EVALUATION_CATEGORY,
    SUM(REVENUE_IMPACT) as REVENUE_IMPACT,
    AVG(TOTAL_CLICKS) as AVG_CLICKS,
    BUSINESS_IMPACT
FROM MODEL_PERFORMANCE_ANALYSIS
GROUP BY USER_TYPE, EVALUATION_CATEGORY, BUSINESS_IMPACT
ORDER BY 
    CASE 
        WHEN EVALUATION_CATEGORY = 'FALSE_POSITIVE' THEN 1  -- Most critical
        WHEN EVALUATION_CATEGORY = 'FALSE_NEGATIVE' THEN 2
        WHEN EVALUATION_CATEGORY = 'TRUE_POSITIVE' THEN 3
        WHEN EVALUATION_CATEGORY = 'TRUE_NEGATIVE' THEN 4
    END,
    REVENUE_IMPACT DESC;

-- ----------------------------------------------------------------------------
-- STEP 6: THRESHOLD OPTIMIZATION ANALYSIS
-- ----------------------------------------------------------------------------
-- Test different thresholds to find optimal balance
-- ----------------------------------------------------------------------------

WITH threshold_analysis AS (
    SELECT 
        threshold,
        SUM(CASE WHEN TOTAL_CLICKS > threshold AND IS_ACTUALLY_BOT = TRUE THEN 1 ELSE 0 END) as TP,
        SUM(CASE WHEN TOTAL_CLICKS <= threshold AND IS_ACTUALLY_BOT = FALSE THEN 1 ELSE 0 END) as TN,
        SUM(CASE WHEN TOTAL_CLICKS > threshold AND IS_ACTUALLY_BOT = FALSE THEN 1 ELSE 0 END) as FP,
        SUM(CASE WHEN TOTAL_CLICKS <= threshold AND IS_ACTUALLY_BOT = TRUE THEN 1 ELSE 0 END) as FN,
        SUM(CASE WHEN TOTAL_CLICKS > threshold AND IS_ACTUALLY_BOT = FALSE THEN REVENUE_IMPACT ELSE 0 END) as FP_REVENUE_LOSS
    FROM TRAFFIC_EVALUATION
    CROSS JOIN (
        SELECT 30 as threshold UNION ALL SELECT 40 UNION ALL SELECT 50 
        UNION ALL SELECT 60 UNION ALL SELECT 70 UNION ALL SELECT 80
    ) thresholds
    GROUP BY threshold
)
SELECT 
    threshold as CLICK_THRESHOLD,
    TP, TN, FP, FN,
    
    -- Metrics
    ROUND(TP * 100.0 / NULLIF(TP + FP, 0), 2) as PRECISION,
    ROUND(TP * 100.0 / NULLIF(TP + FN, 0), 2) as RECALL,
    ROUND(
        2 * (TP * 100.0 / NULLIF(TP + FP, 0)) * (TP * 100.0 / NULLIF(TP + FN, 0)) /
        NULLIF((TP * 100.0 / NULLIF(TP + FP, 0)) + (TP * 100.0 / NULLIF(TP + FN, 0)), 0)
    , 2) as F1_SCORE,
    
    -- Business Impact
    FP as USERS_BLOCKED_INCORRECTLY,
    FP_REVENUE_LOSS as REVENUE_LOSS_USD,
    FN as BOTS_MISSED,
    
    -- Recommendation Score (higher is better)
    ROUND(
        (TP * 100.0 / NULLIF(TP + FP, 0)) * 0.4 +  -- Precision weight
        (TP * 100.0 / NULLIF(TP + FN, 0)) * 0.4 +  -- Recall weight
        (100 - (FP_REVENUE_LOSS / 10)) * 0.2       -- Revenue protection weight
    , 2) as RECOMMENDATION_SCORE
    
FROM threshold_analysis
ORDER BY RECOMMENDATION_SCORE DESC;

-- ----------------------------------------------------------------------------
-- STEP 7: FINAL REPORT
-- ----------------------------------------------------------------------------

SELECT 
    'MODEL_EVALUATION_COMPLETE' as STATUS,
    CURRENT_TIMESTAMP() as ANALYSIS_TIMESTAMP,
    (SELECT COUNT(*) FROM MODEL_PERFORMANCE_ANALYSIS) as TOTAL_IPS_ANALYZED,
    (SELECT COUNT(*) FROM MODEL_PERFORMANCE_ANALYSIS WHERE EVALUATION_CATEGORY = 'TRUE_POSITIVE') as TRUE_POSITIVES,
    (SELECT COUNT(*) FROM MODEL_PERFORMANCE_ANALYSIS WHERE EVALUATION_CATEGORY = 'FALSE_POSITIVE') as FALSE_POSITIVES,
    (SELECT COUNT(*) FROM MODEL_PERFORMANCE_ANALYSIS WHERE EVALUATION_CATEGORY = 'FALSE_NEGATIVE') as FALSE_NEGATIVES,
    (SELECT SUM(REVENUE_IMPACT) FROM MODEL_PERFORMANCE_ANALYSIS WHERE EVALUATION_CATEGORY = 'FALSE_POSITIVE') as ESTIMATED_REVENUE_LOSS;

-- ============================================================================
-- EVALUATION COMPLETE
-- ============================================================================
-- Key Insights:
-- 1. Current threshold (50 clicks) blocks X users incorrectly
-- 2. Estimated revenue loss: $X from false positives
-- 3. X sophisticated bots missed (false negatives)
-- 4. Optimal threshold based on analysis: X clicks
--
-- Recommendations:
-- - Consider multi-factor detection (not just volume)
-- - Implement velocity checks to reduce false positives
-- - Add manual review queue for borderline cases (40-60 clicks)
-- ============================================================================
