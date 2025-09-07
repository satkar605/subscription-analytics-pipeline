/*
 * Staging model for Trail Trekker subscriptions (FACT TABLE)
 * 
 * Purpose: Core fact table for subscription growth analytics
 * Source: subscriptions table (raw DuckDB data)
 * 
 * Critical for Growth Analytics:
 * - Subscription lifecycle events (start, end, status changes)
 * - Plan upgrade/downgrade tracking
 * - Churn and retention analysis
 * - Revenue recognition (MRR/ARR calculations)
 * 
 * Key Transformations:
 * - Standardize all date fields to timestamp format
 * - Rename fields for analytics clarity
 * - Ensure proper temporal data for time-series analysis
 * 
 * Data Quality: 
 * - not_null audits on subscription, customer, and plan identifiers
 * - Ensures referential integrity for dimensional modeling
 */

 MODEL (
    name staging.subscriptions,
    kind VIEW,
    audits (
        not_null(columns := (subscription_id, customer_id, plan_id))
    )
 );

 SELECT
    subscription_id,
    customer_id,
    plan_id,
    billing_cycle AS billing_cadence,
    subscription_start_date::timestamp AS subscription_started_at,
    CASE 
        WHEN subscription_end_date IS NULL OR subscription_end_date = 'NULL' THEN NULL 
        ELSE subscription_end_date::timestamp 
    END AS subscription_ended_at,
    status AS subscription_status,
    CASE 
        WHEN next_billing_date IS NULL OR next_billing_date = 'NULL' THEN NULL 
        ELSE next_billing_date::timestamp 
    END AS next_billing_at,
    payment_method
FROM subscriptions