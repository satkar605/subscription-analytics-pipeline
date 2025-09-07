/*
 * Plan Dimension Table (SCD Type 2)
 * Purpose: Plan hierarchy and pricing for subscription change analytics
 * Source: staging.plans
 * 
 * Key for Madison's Business Question:
 * - "Which plans are most popular to switch to/from?"
 * - Track plan pricing changes over time
 * - Enable revenue impact calculations
 * - Support plan transition matrix analysis
 */

MODEL (
    name dim.plans,
    kind SCD_TYPE_2 (
        unique_key plan_id,
        valid_from_name effective_date,
        valid_to_name expiration_date
    ),
    audits (
        not_null(columns := (plan_key, plan_id, effective_date))
    )
);

SELECT 
    -- Surrogate Key Generation
    ROW_NUMBER() OVER (ORDER BY plan_id) AS plan_key,
    
    -- Natural Business Key
    plan_id,
    
    -- Plan Identity
    plan_name,
    plan_level,
    
    -- Pricing Information (Critical for Revenue Analysis)
    monthly_price_usd,
    
    -- Plan Features
    max_hikes_per_month,
    photo_storage_gb,
    plan_description,
    
    -- Plan Categorization (Business Logic)
    CASE 
        WHEN plan_level = '1' THEN 'Basic'
        WHEN plan_level = '2' THEN 'Premium' 
        WHEN plan_level = '3' THEN 'Pro'
        ELSE 'Unknown'
    END AS plan_tier,
    
    -- Plan Lifecycle
    plan_created_at,
    
    -- SCD Type 2 Temporal Fields
    plan_created_at AS effective_date,
    '9999-12-31'::timestamp AS expiration_date,
    TRUE AS is_current,
    plan_created_at AS updated_at  -- Required for SCD Type 2 change detection
    
FROM staging.plans