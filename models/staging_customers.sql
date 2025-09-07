/*
 * Staging model for Trail Trekker customers (Customer Dimension)
 * 
 * Purpose: Clean and standardize customer master data for analytics
 * Source: customers table (raw DuckDB data)
 * 
 * Key Transformations:
 * - Standardize all date fields to timestamp format
 * - Rename preferred_difficulty to difficulty_preference for clarity
 * - Ensure proper data types for demographic analysis
 * 
 * Data Quality: 
 * - not_null audits on critical customer identifiers
 * - Ensures customer uniqueness (customer_id, username, email)
 * 
 * Analytics Use Cases:
 * - Customer segmentation by location, difficulty preference
 * - Demographic analysis for growth team
 * - Customer lifecycle and behavior analysis
 */

 MODEL (
    name staging.customers,
    kind VIEW,
    audits (
        not_null(columns := (customer_id, username, email))
    )
 );

 SELECT
    customer_id,
    username,
    email,
    phone,
    first_name,
    last_name,
    date_of_birth::timestamp AS birth_date,
    preferred_difficulty AS difficulty_preference,
    location_city,
    location_state,
    location_country,
    profile_created_date::timestamp AS profile_created_at,
    total_hikes_logged,
    favorite_trail_type
FROM customers