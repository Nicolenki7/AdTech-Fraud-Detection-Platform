"""
AdTech Fraud Detection Platform - UDF Deployment
=================================================
Purpose: Deploy Python UDF for fraud classification
Author: Nicolas Zalazar
Version: 2.0 (Unified Platform)
Date: 2026-02-28

This module:
1. Defines classification logic (BLOCK/REVIEW/MONITOR)
2. Registers Python UDF in Snowflake
3. Applies UDF to risk-scored data
4. Generates actionable alerts table
"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, udf, when, lit, concat_ws
from snowflake.snowpark.types import StringType, FloatType, BooleanType
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def determine_action(risk_score: float, confidence: float, fraud_flags: list) -> str:
    """
    Classification logic for fraud action determination.
    
    Decision Matrix:
    ----------------
    | Risk Score | Confidence | Action                  |
    |------------|------------|-------------------------|
    | >= 0.90    | Any        | BLOCK_IP_IMMEDIATELY    |
    | 0.70-0.89  | >= 0.80    | BLOCK_IP_IMMEDIATELY    |
    | 0.70-0.89  | < 0.80     | SEND_TO_MANUAL_REVIEW   |
    | 0.40-0.69  | Any        | SEND_TO_MANUAL_REVIEW   |
    | < 0.40     | Any        | MONITOR_PASSIVELY       |
    
    Args:
        risk_score: Float 0.0-1.0
        confidence: Float 0.0-1.0
        fraud_flags: List of detected fraud indicators
    
    Returns:
        Action string: BLOCK_IP_IMMEDIATELY | SEND_TO_MANUAL_REVIEW | MONITOR_PASSIVELY
    """
    
    # Handle null/None values
    if risk_score is None:
        return "ERROR_MISSING_SCORE"
    if confidence is None:
        confidence = 0.5
    
    # Critical risk: Block immediately regardless of confidence
    if risk_score >= 0.90:
        return "BLOCK_IP_IMMEDIATELY"
    
    # High risk with high confidence: Block
    if risk_score >= 0.70 and confidence >= 0.80:
        return "BLOCK_IP_IMMEDIATELY"
    
    # High risk with low confidence OR medium risk: Manual review
    if risk_score >= 0.70 and confidence < 0.80:
        return "SEND_TO_MANUAL_REVIEW"
    
    if 0.40 <= risk_score < 0.70:
        return "SEND_TO_MANUAL_REVIEW"
    
    # Low risk: Passive monitoring
    return "MONITOR_PASSIVELY"


def determine_priority(risk_score: float, action: str) -> str:
    """
    Determine alert priority based on risk and action.
    """
    if action == "BLOCK_IP_IMMEDIATELY" and risk_score >= 0.95:
        return "CRITICAL"
    elif action == "BLOCK_IP_IMMEDIATELY":
        return "HIGH"
    elif action == "SEND_TO_MANUAL_REVIEW" and risk_score >= 0.60:
        return "MEDIUM"
    else:
        return "LOW"


def generate_evidence_summary(
    risk_score: float,
    fraud_reasons: list,
    total_clicks: int,
    fastest_click_ms: int,
    country_code: str
) -> str:
    """
    Generate human-readable evidence summary for analysts.
    """
    evidence_parts = []
    
    if risk_score >= 0.90:
        evidence_parts.append(f"Critical risk ({risk_score:.2f})")
    elif risk_score >= 0.70:
        evidence_parts.append(f"High risk ({risk_score:.2f})")
    
    if fraud_reasons:
        evidence_parts.append(f"Flags: {', '.join(fraud_reasons[:3])}")
    
    if total_clicks:
        evidence_parts.append(f"{total_clicks:,} clicks")
    
    if fastest_click_ms and fastest_click_ms < 100:
        evidence_parts.append(f"Ultra-fast clicks ({fastest_click_ms}ms)")
    
    if country_code:
        evidence_parts.append(f"Country: {country_code}")
    
    return " | ".join(evidence_parts) if evidence_parts else "Standard risk profile"


def deploy_classification_udf(session: Session) -> None:
    """
    Deploy Python UDF and generate actionable alerts.
    """
    
    logger.info("Starting UDF deployment pipeline...")
    
    # Set context
    session.use_database("FRAUD_DETECTION_DB")
    session.use_schema("RAW_LOGS")
    
    # -------------------------------------------------------------------------
    # STEP 1: LOAD RISK-SCORED DATA
    # -------------------------------------------------------------------------
    logger.info("Loading risk-scored data from FINAL_RISK_SCORES...")
    
    df_scores = session.table("FRAUD_DETECTION_DB.RAW_LOGS.FINAL_RISK_SCORES")
    
    initial_count = df_scores.count()
    logger.info(f"Loaded {initial_count} risk-scored records")
    
    if initial_count == 0:
        logger.warning("No risk scores found. Run feature engineering first.")
        return
    
    # -------------------------------------------------------------------------
    # STEP 2: REGISTER PYTHON UDF
    # -------------------------------------------------------------------------
    logger.info("Registering DECISION_LOGIC_UDF in Snowflake...")
    
    # Register the UDF
    decision_udf = udf(
        func=determine_action,
        return_type=StringType(),
        input_types=[FloatType(), FloatType()],
        name="DECISION_LOGIC_UDF",
        replace=True,
        is_permanent=True,  # Make it permanent for reuse
        stage_location="@FRAUD_DETECTION_DB.RAW_LOGS.UDF_STAGE",
        packages=["snowflake-snowpark-python"]
    )
    
    logger.info("UDF registered successfully")
    
    # -------------------------------------------------------------------------
    # STEP 3: APPLY UDF TO GENERATE ACTIONS
    # -------------------------------------------------------------------------
    logger.info("Applying classification UDF...")
    
    df_with_actions = df_scores.withColumn(
        "FINAL_ACTION",
        decision_udf(col("RISK_SCORE"), col("CONFIDENCE_LEVEL"))
    ).withColumn(
        "PRIORITY",
        when(col("FINAL_ACTION") == "BLOCK_IP_IMMEDIATELY", 
            when(col("RISK_SCORE") >= 0.95, lit("CRITICAL"))
            .otherwise(lit("HIGH")))
        .when(col("FINAL_ACTION") == "SEND_TO_MANUAL_REVIEW",
            when(col("RISK_SCORE") >= 0.60, lit("MEDIUM"))
            .otherwise(lit("LOW")))
        .otherwise(lit("LOW"))
    )
    
    # -------------------------------------------------------------------------
    # STEP 4: GENERATE EVIDENCE SUMMARY
    # -------------------------------------------------------------------------
    logger.info("Generating evidence summaries...")
    
    # For simplicity, we'll use a basic summary (full implementation would use another UDF)
    df_with_evidence = df_with_actions.withColumn(
        "EVIDENCE_SUMMARY",
        concat_ws(
            " | ",
            when(col("RISK_SCORE") >= 0.90, lit(f"Critical risk ({col('RISK_SCORE')})"))
            .when(col("RISK_SCORE") >= 0.70, lit(f"High risk ({col('RISK_SCORE')})"))
            .otherwise(lit("Standard risk")),
            col("COUNTRY_CODE"),
            col("total_clicks").cast(StringType())
        )
    )
    
    # -------------------------------------------------------------------------
    # STEP 5: SET ALERT EXPIRATION
    # -------------------------------------------------------------------------
    logger.info("Setting alert expiration timestamps...")
    
    # Alerts expire after 7 days for BLOCK, 3 days for REVIEW, 1 day for MONITOR
    df_final = df_with_evidence.withColumn(
        "EXPIRES_AT",
        when(col("FINAL_ACTION") == "BLOCK_IP_IMMEDIATELY",
            lit(datetime.utcnow() + timedelta(days=7)))
        .when(col("FINAL_ACTION") == "SEND_TO_MANUAL_REVIEW",
            lit(datetime.utcnow() + timedelta(days=3)))
        .otherwise(lit(datetime.utcnow() + timedelta(days=1)))
    )
    
    # -------------------------------------------------------------------------
    # STEP 6: SELECT FINAL COLUMNS
    # -------------------------------------------------------------------------
    logger.info("Selecting final columns for ACTIONABLE_ALERTS...")
    
    df_alerts = df_final.select(
        lit("ALERT-").concat(col("IP_ADDRESS")).concat(lit("-")).concat(
            lit(datetime.utcnow().strftime("%Y%m%d%H%M%S"))
        ).alias("ALERT_ID"),
        col("IP_ADDRESS"),
        col("FINAL_ACTION"),
        col("PRIORITY"),
        col("RISK_SCORE"),
        col("RISK_LEVEL"),
        lit("Automated detection via multi-factor analysis").alias("FRAUD_REASON"),
        col("EVIDENCE_SUMMARY"),
        lit("PENDING").alias("ACTION_STATUS"),
        lit(None).alias("ACTIONED_BY"),
        lit(None).alias("ACTIONED_AT"),
        lit(datetime.utcnow()).alias("CREATED_AT"),
        col("EXPIRES_AT"),
        col("CONFIDENCE_LEVEL"),
        col("COUNTRY_CODE"),
        col("total_clicks"),
        col("fastest_click_ms")
    )
    
    # -------------------------------------------------------------------------
    # STEP 7: SAVE TO SNOWFLAKE
    # -------------------------------------------------------------------------
    logger.info("Saving actionable alerts to ACTIONABLE_ALERTS table...")
    
    df_alerts.write.mode("overwrite").save_as_table(
        "FRAUD_DETECTION_DB.RAW_LOGS.ACTIONABLE_ALERTS"
    )
    
    final_count = df_alerts.count()
    logger.info(f"Successfully created {final_count} actionable alerts")
    
    # -------------------------------------------------------------------------
    # STEP 8: SUMMARY STATISTICS
    # -------------------------------------------------------------------------
    logger.info("Generating alert summary statistics...")
    
    summary = df_alerts.groupBy("FINAL_ACTION").agg(
        {"IP_ADDRESS": "count", "RISK_SCORE": "avg"}
    ).collect()
    
    priority_summary = df_alerts.groupBy("PRIORITY").agg(
        {"IP_ADDRESS": "count"}
    ).collect()
    
    print("\n" + "="*60)
    print("UDF CLASSIFICATION COMPLETE - SUMMARY")
    print("="*60)
    print(f"Total Alerts Generated: {final_count}")
    print("\nAction Distribution:")
    for row in summary:
        action = row['FINAL_ACTION']
        count = row['COUNT(IP_ADDRESS)']
        avg_score = row['AVG(RISK_SCORE)']
        print(f"  {action}: {count} alerts (Avg Score: {avg_score:.3f})")
    
    print("\nPriority Distribution:")
    for row in priority_summary:
        print(f"  {row['PRIORITY']}: {row['COUNT(IP_ADDRESS)']} alerts")
    
    # Immediate action required
    block_count = df_alerts.filter(col("FINAL_ACTION") == "BLOCK_IP_IMMEDIATELY").count()
    review_count = df_alerts.filter(col("FINAL_ACTION") == "SEND_TO_MANUAL_REVIEW").count()
    
    print(f"\n🚨 Immediate Blocks Required: {block_count}")
    print(f"📋 Manual Reviews Pending: {review_count}")
    print("="*60 + "\n")
    
    return df_alerts


def main(session: Session):
    """
    Entry point for Snowpark stored procedure execution.
    """
    try:
        deploy_classification_udf(session)
        return {"status": "success", "message": "UDF deployment completed successfully"}
    except Exception as e:
        logger.error(f"UDF deployment failed: {str(e)}")
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    print("This module is designed to run as a Snowpark stored procedure.")
    print("Execute via: python scripts/04_udf_deployment.py")
