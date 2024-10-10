SELECT
    lead_id, --hash phone number and address
    source_file,
    '2024-10-10' AS salesforce_loaded_date, --hard coded for now, but this would come from a join to the salesforce source data on the lead id to get the date it was loaded into salesforce
    file_loaded_at::DATE AS effective_start_date,
    LEAD(effective_start_date,1) OVER (PARTITION BY lead_id) - 1 AS effective_end_date, --find the next file load date, if it doesn't exist, populate null
    company,
    full_name,
    first_name,
    last_name,
    title,
    email,
    full_address,
    street_address1,
    street_address2,
    city,
    state,
    zip,
    county,
    phone,
    license_issued_date,
    license_expiration_date,
    license_number,
    license_type,
    accepts_financial_aid,
    infant_age_served,
    toddler_age_served,
    preschool_age_served,
    school_age_served,
    capacity
FROM
    {{ ref('fct_leads') }}
