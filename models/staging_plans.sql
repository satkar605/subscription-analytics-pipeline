/*
 * Staging model for Trail Trekker subscription plans
 * 
 * Purpose: Clean and standardize plan data for downstream analytics
 * Source: plans table (raw DuckDB data)
 * 
 * Transformations applied:
 * - Rename description to plan_description for clarity
 * - Cast created_at to proper timestamp format
 * - Rename price to monthly_price_usd for clarity
 * 
 * Data Quality: 
 * - not_null audits on critical plan identifiers
 * - Ensures plan hierarchy integrity (plan_id, plan_name, plan_level)
 */

 MODEL (
    name staging.plans,
    kind VIEW,
    audits (
        not_null(columns := (plan_id, plan_name, plan_level))
    )
 );

 SELECT
    plan_id,
    plan_name,
    plan_level,
    price AS monthly_price_usd,
    max_hikes_per_month,
    photo_storage_gb,
    description AS plan_description,
    created_at::timestamp AS plan_created_at
 FROM plans