/*
This script cleans WIC clinic address data
Written by: Rashida Brown

*/



CREATE TABLE wic_add_full_fin AS 
    SELECT *,
        CASE WHEN "BusinessSt" LIKE ('%Box%') THEN ''
            WHEN "BusinessSt" NOT LIKE ('%Box%') 
            THEN (rtrim("BusinessSt", ' ') || ', ' || 
                  rtrim("BusinessCity", ' ') || ', ' || 
                  rtrim("BusinessState", ' ') || ', ' || 
                  rtrim("BusinessPostalCode"))
        END AS add_geo2, 
        concat("Cstone ID", '_', rn) AS unique_cs_id FROM
        (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY "Cstone ID") AS rn 
                FROM wic_clinics
        ) AS tmp;

CREATE TABLE fcm_add_full_fin AS 
    SELECT *, 
        rtrim("address", ' ') || ', ' || rtrim("City", ' ') || 
        ', IL, ' || trim(to_char("Zip", '99999'), ' ') AS add_geo2
        FROM fcm_clinics;

CREATE TABLE bbo_clin_add_full_fin AS 
    SELECT *, 
        rtrim("ADDRESS", ' ') || ', ' || rtrim("CITY", ' ') || ', ' ||
        rtrim("STATE", ' ') || ', ' || rtrim("ZIP", ' ') AS add_geo2
        FROM bbo_clinics;
