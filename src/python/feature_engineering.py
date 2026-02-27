"""
AdTech Fraud Detection Platform - Feature Engineering
======================================================
Purpose: Snowpark-based feature engineering and risk scoring
Author: Nicolas Zalazar
Version: 2.0 (Unified Platform)
Date: 2026-02-28

This module:
1. Loads suspect IPs from SQL filtering phase
2. Applies multi-factor risk scoring
3. Enriches with geographic and device data
4. Saves final risk scores to Snowflake table
"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, when, lit, concat, array_size, coalesce
)
from snowflake.snowpark.types import FloatType, StringType, BooleanType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_risk_score_features(session: Session) -> None:
    """
    Main feature engineering pipeline.
    
    Risk Score Calculation:
    -----------------------
    Final Risk Score = Σ(feature_weight × feature_value)
    
    Features & Weights:
    - HIGH_VELOCITY_BOT: 0.40 (fastest indicator of automation)
    - UA_ROTATION_BOT: 0.30 (strong bot signal)
    - EXTREME_HIGH_VOLUME: 0.25 (could be legitimate power user)
    - HIGH_VOLUME_SUSPECT: 0.20 (needs additional context)
    - SUSPICIOUS_COUNTRY: 0.10 (geo-risk factor)
    - DEVICE_ANOMALY: 0.05 (minor signal)
    
    Risk Levels:
    - CRITICAL: score >= 0.90
    - HIGH: 0.70 <= score < 0.90
    - MEDIUM: 0.40 <= score < 0.70
    - LOW: score < 0.40
    """
    
    logger.info("Starting feature engineering pipeline...")
    
    # Set context
    session.use_database("FRAUD_DETECTION_DB")
    session.use_schema("RAW_LOGS")
    
    # -------------------------------------------------------------------------
    # STEP 1: LOAD SUSPECT DATA
    # -------------------------------------------------------------------------
    logger.info("Loading suspect IPs from SUSPECT_IPS table...")
    
    df_suspects = session.table("FRAUD_DETECTION_DB.RAW_LOGS.SUSPECT_IPS")
    
    initial_count = df_suspects.count()
    logger.info(f"Loaded {initial_count} suspect IPs")
    
    if initial_count == 0:
        logger.warning("No suspects found. Run SQL filtering phase first.")
        return
    
    # -------------------------------------------------------------------------
    # STEP 2: BASE RISK SCORE (from fraud reason)
    # -------------------------------------------------------------------------
    logger.info("Calculating base risk scores from fraud reasons...")
    
    df_scored = df_suspects.withColumn(
        "BASE_RISK_SCORE",
        when(col("primary_fraud_reason") == "INHUMAN_VELOCITY_BOT", lit(0.98))
        .when(col("primary_fraud_reason") == "HIGH_VELOCITY_BOT", lit(0.90))
        .when(col("primary_fraud_reason") == "EXTREME_HIGH_VOLUME", lit(0.85))
        .when(col("primary_fraud_reason") == "AGGRESSIVE_UA_ROTATION", lit(0.82))
        .when(col("primary_fraud_reason") == "HIGH_VOLUME_SUSPECT", lit(0.65))
        .when(col("primary_fraud_reason") == "UA_ROTATION_BOT", lit(0.75))
        .when(col("primary_fraud_reason") == "CAMPAIGN_SPAMMER", lit(0.70))
        .when(col("primary_fraud_reason") == "SUSPICIOUS_VELOCITY", lit(0.55))
        .when(col("primary_fraud_reason") == "VELOCITY_ANOMALY", lit(0.45))
        .when(col("primary_fraud_reason") == "VOLUME_ANOMALY", lit(0.40))
        .otherwise(lit(0.35))
    )
    
    # -------------------------------------------------------------------------
    # STEP 3: VELOCITY-BASED ADJUSTMENTS
    # -------------------------------------------------------------------------
    logger.info("Applying velocity-based risk adjustments...")
    
    df_scored = df_scored.withColumn(
        "VELOCITY_SCORE",
        when(col("fastest_click_ms").isNotNull(),
            when(col("fastest_click_ms") < 10, lit(1.0))      # Inhuman speed
            .when(col("fastest_click_ms") < 50, lit(0.9))     # Bot-like
            .when(col("fastest_click_ms") < 100, lit(0.7))    # Suspicious
            .when(col("fastest_click_ms") < 200, lit(0.5))    # Fast but possible
            .otherwise(lit(0.3))                               # Normal
        ).otherwise(lit(0.2))  # No velocity data
    )
    
    # -------------------------------------------------------------------------
    # STEP 4: VOLUME-BASED ADJUSTMENTS
    # -------------------------------------------------------------------------
    logger.info("Applying volume-based risk adjustments...")
    
    df_scored = df_scored.withColumn(
        "VOLUME_SCORE",
        when(col("total_clicks") >= 10000, lit(1.0))
        .when(col("total_clicks") >= 5000, lit(0.85))
        .when(col("total_clicks") >= 1000, lit(0.70))
        .when(col("total_clicks") >= 500, lit(0.55))
        .when(col("total_clicks") >= 100, lit(0.40))
        .otherwise(lit(0.25))
    )
    
    # -------------------------------------------------------------------------
    # STEP 5: UA ROTATION SCORE
    # -------------------------------------------------------------------------
    logger.info("Calculating UA rotation risk score...")
    
    df_scored = df_scored.withColumn(
        "UA_ROTATION_SCORE",
        when(col("distinct_uas").isNotNull(),
            when(col("distinct_uas") >= 20, lit(1.0))
            .when(col("distinct_uas") >= 10, lit(0.85))
            .when(col("distinct_uas") >= 5, lit(0.70))
            .when(col("distinct_uas") >= 3, lit(0.50))
            .otherwise(lit(0.20))
        ).otherwise(lit(0.1))
    )
    
    # -------------------------------------------------------------------------
    # STEP 6: COMBINED RISK SCORE (Weighted Average)
    # -------------------------------------------------------------------------
    logger.info("Calculating combined risk score...")
    
    df_scored = df_scored.withColumn(
        "RISK_SCORE",
        # Weighted combination
        (col("BASE_RISK_SCORE") * lit(0.40) +
         col("VELOCITY_SCORE") * lit(0.30) +
         col("VOLUME_SCORE") * lit(0.20) +
         col("UA_ROTATION_SCORE") * lit(0.10))
    )
    
    # Ensure score is between 0 and 1
    df_scored = df_scored.withColumn(
        "RISK_SCORE",
        when(col("RISK_SCORE") > 1.0, lit(1.0))
        .when(col("RISK_SCORE") < 0.0, lit(0.0))
        .otherwise(col("RISK_SCORE"))
    )
    
    # -------------------------------------------------------------------------
    # STEP 7: RISK LEVEL CLASSIFICATION
    # -------------------------------------------------------------------------
    logger.info("Classifying risk levels...")
    
    df_scored = df_scored.withColumn(
        "RISK_LEVEL",
        when(col("RISK_SCORE") >= 0.90, lit("CRITICAL"))
        .when(col("RISK_SCORE") >= 0.70, lit("HIGH"))
        .when(col("RISK_SCORE") >= 0.40, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )
    
    # -------------------------------------------------------------------------
    # STEP 8: FRAUD FLAG (Binary Classification)
    # -------------------------------------------------------------------------
    logger.info("Generating binary fraud flag...")
    
    df_scored = df_scored.withColumn(
        "FRAUD_FLAG",
        when(col("RISK_SCORE") >= 0.70, lit(True))
        .otherwise(lit(False))
    )
    
    # -------------------------------------------------------------------------
    # STEP 9: CONFIDENCE LEVEL
    # -------------------------------------------------------------------------
    logger.info("Calculating confidence level...")
    
    # Confidence based on data completeness and signal strength
    df_scored = df_scored.withColumn(
        "CONFIDENCE_LEVEL",
        when(
            (col("fastest_click_ms").isNotNull()) & 
            (col("distinct_uas").isNotNull()) &
            (col("total_clicks") >= 1000),
            lit(0.95)  # High confidence: all signals present
        )
        .when(
            (col("fastest_click_ms").isNotNull()) | 
            (col("distinct_uas").isNotNull()),
            lit(0.75)  # Medium confidence: partial signals
        )
        .otherwise(lit(0.50))  # Low confidence: limited data
    )
    
    # -------------------------------------------------------------------------
    # STEP 10: SELECT FINAL COLUMNS
    # -------------------------------------------------------------------------
    logger.info("Selecting final columns for output...")
    
    df_final = df_scored.select(
        col("IP_ADDRESS"),
        col("RISK_SCORE").cast(FloatType()).alias("RISK_SCORE"),
        col("RISK_LEVEL").cast(StringType()).alias("RISK_LEVEL"),
        col("VELOCITY_SCORE").cast(FloatType()).alias("VELOCITY_SCORE"),
        col("VOLUME_SCORE").cast(FloatType()).alias("VOLUME_SCORE"),
        col("UA_ROTATION_SCORE").cast(FloatType()).alias("UA_ROTATION_SCORE"),
        col("FRAUD_FLAG").cast(BooleanType()).alias("FRAUD_FLAG"),
        col("CONFIDENCE_LEVEL").cast(FloatType()).alias("CONFIDENCE_LEVEL"),
        col("COUNTRY_CODE"),
        col("DEVICE_TYPE"),
        col("total_clicks"),
        col("distinct_uas"),
        col("fastest_click_ms"),
        col("fraud_reasons"),
        col("detection_methods"),
        lit(True).alias("IS_PROCESSED"),
        col("detected_at"),
        lit(session.create_dataframe([1]).selectExpr("CURRENT_TIMESTAMP() as ts").collect()[0][0]).alias("SCORED_AT")
    )
    
    # -------------------------------------------------------------------------
    # STEP 11: SAVE TO SNOWFLAKE
    # -------------------------------------------------------------------------
    logger.info("Saving results to FINAL_RISK_SCORES table...")
    
    df_final.write.mode("overwrite").save_as_table(
        "FRAUD_DETECTION_DB.RAW_LOGS.FINAL_RISK_SCORES"
    )
    
    final_count = df_final.count()
    logger.info(f"Successfully saved {final_count} risk-scored records")
    
    # -------------------------------------------------------------------------
    # STEP 12: SUMMARY STATISTICS
    # -------------------------------------------------------------------------
    logger.info("Generating summary statistics...")
    
    summary = df_final.groupBy("RISK_LEVEL").agg(
        {"RISK_SCORE": "avg", "IP_ADDRESS": "count"}
    ).collect()
    
    print("\n" + "="*60)
    print("FEATURE ENGINEERING COMPLETE - SUMMARY")
    print("="*60)
    print(f"Total IPs Processed: {final_count}")
    print("\nRisk Level Distribution:")
    for row in summary:
        print(f"  {row['RISK_LEVEL']}: {row['COUNT(IP_ADDRESS)']} IPs (Avg Score: {row['AVG(RISK_SCORE)']:.3f})")
    
    # Critical alerts requiring immediate action
    critical_count = df_final.filter(col("RISK_LEVEL") == "CRITICAL").count()
    high_count = df_final.filter(col("RISK_LEVEL") == "HIGH").count()
    
    print(f"\n🚨 CRITICAL Alerts: {critical_count}")
    print(f"⚠️  HIGH Alerts: {high_count}")
    print(f"📋 Action Required: {critical_count + high_count} IPs")
    print("="*60 + "\n")
    
    return df_final


def main(session: Session):
    """
    Entry point for Snowpark stored procedure execution.
    """
    try:
        create_risk_score_features(session)
        return {"status": "success", "message": "Feature engineering completed successfully"}
    except Exception as e:
        logger.error(f"Feature engineering failed: {str(e)}")
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    # For local testing
    print("This module is designed to run as a Snowpark stored procedure.")
    print("Execute via: python scripts/03_feature_engineering.py")
