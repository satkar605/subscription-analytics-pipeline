/*
 * Customer Dimension Table (SCD Type 2)
 * Purpose: Track customer attributes over time for growth analytics
 * Source: staging.customers
 */

MODEL (
    name dim.customers,
    kind SCD_TYPE_2 (
        unique_key customer_id,           -- Business key for tracking changes
        valid_from_name effective_date,    -- When this version became active
        valid_to_name expiration_date      -- When this version expired
    ),
    audits (
        not_null(columns := (customer_id, effective_date))
    )
);

SELECT
    -- Surrogate Key Generation
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    
    -- Natural Business Key
    customer_id,
    
    -- Customer Identity Attributes
    username,
    email,
    first_name,
    last_name,
    birth_date,
    
    -- Customer Segmentation Attributes
    difficulty_preference,
    location_city,
    location_state,
    location_country,
    
    -- Behavioral Attributes
    total_hikes_logged,
    favorite_trail_type,
    
    -- Customer Lifecycle
    profile_created_at,
    profile_created_at AS updated_at,  -- Required for SCD Type 2 change detection

    -- SCD Type 2 Temporal Fields
    profile_created_at AS effective_date,
    '9999-12-31'::timestamp AS expiration_date,
    TRUE AS is_current
    
FROM staging.customers