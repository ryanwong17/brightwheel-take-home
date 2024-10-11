{{
    config(
        materialized='incremental'
    )
}}

SELECT
    '2024-10-10' AS file_loaded_at, --airflow/system generated, hardcoded for exercise
    'source1.csv' AS source_file, --would make this dynamic for production
    name AS company,
    "Primary Contact Name" AS full_name,
    CASE
        WHEN --if multiple first names, populate NULL
            full_name ILIKE '% AND %' OR
            full_name ILIKE '% & %' OR
            full_name ILIKE '%,%'
        THEN NULL
        ELSE split_part(full_name,' ',1) --else populate the first name
    END AS first_name,
    CASE
        WHEN --if multiple last names, populate NULL
            full_name ILIKE '% AND %' OR
            full_name ILIKE '% & %' OR
            full_name ILIKE '%,%'
        THEN NULL
        ELSE split_part(full_name,' ',-1) --else populate the last name
    END AS last_name,
    "Primary Contact Role" AS title,
    address AS full_address,
    county AS county,
    state AS state,
    TRIM(SPLIT_PART(SPLIT_PART(address, ',', 2), ' ', 3)) AS zip,
    REGEXP_REPLACE(phone, '[^0-9]', '', 'g') AS phone,
    "First Issue Date" AS license_issued_date,
    "Expiration Date" AS license_expiration_date,
    "Credential Number" AS license_number,
    "Credential Type" AS license_type,
    CASE
        WHEN LOWER("Credential Type") IN ('center','center (provisional)') then 'Center'
        WHEN LOWER("Credential Type") = 'family care' then 'Home'
        ELSE 'Other'
    END as facility_type

FROM
    {{ ref('source1') }}

{% if is_incremental() %}
WHERE
    file_loaded_at >= (SELECT MAX(file_loaded_at) from {{ this }} )

{% endif %}