{{
    config(
        materialized='incremental'
    )
}}

SELECT
    '2024-10-10' AS file_loaded_at, --airflow/system generated, hardcoded for exercise
    'source3.csv' AS source_file, --would make this dynamic for production
    "Operation Name" AS company,
    "Email Address" AS email,
    CONCAT(address,' ',city,', ',state,' ',zip) AS full_address, --create full address field
    address AS street_address1,
    city AS city,
    county AS county,
    state AS state,
    zip AS zip,
    REGEXP_REPLACE(phone, '[^0-9]', '', 'g') AS phone,
    "Issue Date" AS license_issued_date,
    operation AS license_number,
    type AS license_type,
    CASE WHEN infant = 'Y' THEN TRUE ELSE FALSE END AS infant_age_served,
    CASE WHEN toddler = 'Y' THEN TRUE ELSE FALSE END AS toddler_age_served,
    CASE WHEN preschool = 'Y' THEN TRUE ELSE FALSE END AS preschool_age_served,
    CASE WHEN school = 'Y' THEN TRUE ELSE FALSE END AS school_age_served,
     CASE
        WHEN LOWER(type) = 'licensed child-care home' then 'Home'
        WHEN LOWER(type) = 'licensed center - child care program' then 'Center'
        ELSE 'Other'
    END as facility_type
FROM
    {{ ref('source3') }}

 {% if is_incremental() %}
WHERE
    file_loaded_at >= (SELECT MAX(file_loaded_at) from {{ this }} )

{% endif %}