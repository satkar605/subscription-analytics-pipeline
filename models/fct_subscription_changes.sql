/*
 * Subscription Changes Fact Table (Transaction Pattern)
 * Purpose: Capture subscription change events at atomic grain
 * Source: staging.subscriptions
 * 
 * Madison's Business Question:
 * "Which plans are most popular to switch to/from and why?"
 * 
 * Grain: One row per customer per subscription per plan change event
 */

MODEL (
    name fct.subscription_changes,
    kind VIEW,  -- Start simple, optimize later
    audits (
        not_null(columns := (subscription_change_id, customer_id, change_date))
    )
);

-- Step 1: Identify subscription changes using LAG functions
WITH subscription_events AS (
    SELECT 
        s.*,
        -- Get previous subscription for comparison
        LAG(plan_id) OVER (
            PARTITION BY customer_id 
            ORDER BY subscription_started_at
        ) AS previous_plan_id,
        LAG(subscription_status) OVER (
            PARTITION BY customer_id 
            ORDER BY subscription_started_at
        ) AS previous_status,
        LAG(subscription_started_at) OVER (
            PARTITION BY customer_id 
            ORDER BY subscription_started_at
        ) AS previous_start_date
    FROM staging.subscriptions s
),

-- Step 2: Classify change types
subscription_changes AS (
    SELECT 
        se.*,
        -- Identify what type of change occurred
        CASE 
            WHEN previous_plan_id IS NULL THEN 'NEW_SUBSCRIPTION'
            WHEN subscription_status = 'cancelled' THEN 'CANCELLATION'
            WHEN plan_id != previous_plan_id THEN 'PLAN_CHANGE'
            ELSE 'NO_CHANGE'
        END AS change_type
    FROM subscription_events se
    WHERE previous_plan_id IS NOT NULL  -- Exclude new subscriptions per Madison's rules
)

-- Step 3: Build the fact table
SELECT 
    -- Surrogate Key
    ROW_NUMBER() OVER (ORDER BY customer_id, subscription_started_at) AS subscription_change_id,
    
    -- Natural Keys
    subscription_id,
    customer_id,
    
    -- Change Details
    change_type,
    subscription_started_at AS change_date,
    previous_plan_id AS from_plan_id,
    plan_id AS to_plan_id,
    
    -- Basic Measures
    DATEDIFF('day', previous_start_date, subscription_started_at) AS days_on_previous_plan,
    
    -- Change Flags
    CASE WHEN change_type = 'PLAN_CHANGE' THEN 1 ELSE 0 END AS is_plan_change,
    CASE WHEN change_type = 'CANCELLATION' THEN 1 ELSE 0 END AS is_cancellation

FROM subscription_changes 
WHERE change_type != 'NO_CHANGE'