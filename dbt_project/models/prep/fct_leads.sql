{{
    config(
        materialized='incremental'
    )
}}

WITH unioned_cte AS (
SELECT
    *
FROM
{{ dbt_utils.union_relations(
    relations=[ref('stg_source1'),ref('stg_source2'),ref('stg_source3')]
) }}
)

SELECT
    MD5(CONCAT(phone,LOWER(REGEXP_REPLACE(full_address, '[^a-zA-Z0-9]', '')))) AS lead_id, --hash phone number and address
    source_file,
    file_loaded_at,
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
    unioned_cte

  {% if is_incremental() %}
WHERE
    file_loaded_at >= (SELECT MAX(file_loaded_at) from {{ this }} )

{% endif %}


