{{
    config(
        materialized='incremental'
    )
}}

SELECT
    '2024-10-10' AS file_loaded_at, --airflow/system generated, hardcoded for exercise
    'source2.csv' AS source_file, --would make this dynamic for production
    company AS company,
    TRIM(SPLIT_PART("Primary Caregiver", E'\n',1)) AS full_name, --first line contains the full_name
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
    TRIM(SPLIT_PART("Primary Caregiver", E'\n',3)) AS title, --3rd line contains the title
    CONCAT(address1,' ',city,', ',state,' ',zip) AS full_address, --create full address field
    address1 AS street_address1,
    address2 AS street_address2,
    city AS city,
    state AS state,
    zip AS zip,
    REGEXP_REPLACE(phone, '[^0-9]', '', 'g') AS phone,
    CASE
        WHEN "Accepts Subsidy" = 'Accepts Subsidy' THEN TRUE
        ELSE FALSE
    END AS accepts_financial_aid,
    SUBSTRING("License Monitoring Since" FROM POSITION('Monitoring since ' IN "License Monitoring Since") + LENGTH('Monitoring since ')) AS license_issued_date,
    SPLIT_PART("Type License",' - ',2) AS license_number,
    SPLIT_PART("Type License",' - ',1) AS license_type,
    CASE
        WHEN "Ages Accepted 1" ILIKE '%infant%' OR
            aa2 ILIKE '%infant%' OR
            aa3  ILIKE '%infant%' OR
            aa4  ILIKE '%infant%'
        THEN TRUE
        ELSE FALSE
    END AS infant_age_served,
    CASE
        WHEN "Ages Accepted 1" ILIKE '%toddler%' OR
            aa2 ILIKE '%toddler%' OR
            aa3  ILIKE '%toddler%' OR
            aa4  ILIKE '%toddler%'
        THEN TRUE
        ELSE FALSE
    END AS toddler_age_served,
       CASE
        WHEN "Ages Accepted 1" ILIKE '%preschool%' OR
            aa2 ILIKE '%preschool%' OR
            aa3  ILIKE '%preschool%' OR
            aa4  ILIKE '%preschool%'
        THEN TRUE
        ELSE FALSE
    END AS preschool_age_served,
       CASE
        WHEN "Ages Accepted 1" ILIKE '%school%' OR
            aa2 ILIKE '%school%' OR
            aa3  ILIKE '%school%' OR
            aa4  ILIKE '%school%'
        THEN TRUE
        ELSE FALSE
    END AS school_age_served,
    "Total Cap" AS capacity,
     CASE
        WHEN LOWER(license_type) = 'child care family' then 'Home'
        WHEN LOWER(license_type) = 'child care center' then 'Center'
        ELSE 'Other'
    END as facility_type

FROM
    {{ ref('source2') }}

 {% if is_incremental() %}
WHERE
    file_loaded_at >= (SELECT MAX(file_loaded_at) from {{ this }} )

{% endif %}