/*
This script counts the number of public clinics in each census tract
Written by: Rashida Brown

*/

--COUNT WIC CLINICS;
CREATE TABLE wic_agg AS
    SELECT unique_cs_id,
        SUBSTRING(wic_add_geocode2."FIPS_BLK_GEO" FROM 1 FOR 11) 
            AS "FIPS_TRACT_GEO"
    FROM wic_add_geocode2;

CREATE TABLE wic_agg2 AS
    SELECT "FIPS_TRACT_GEO",
        COUNT(unique_cs_id) AS tct_total_wic_clin_n
        FROM wic_agg
        GROUP BY 1;

--COUNT BBO CLINICS;
CREATE TABLE bbo_agg AS
    SELECT "CLINICID_I",
        SUBSTRING(bbo_clinic_geocode2."FIPS_BLK_GEO" FROM 1 FOR 11) 
            AS "FIPS_TRACT_GEO"
        FROM bbo_clinic_geocode2;

CREATE TABLE bbo_agg2 AS
    SELECT "FIPS_TRACT_GEO",
        COUNT("CLINICID_I") AS tct_total_bbo_clin_n
        FROM bbo_agg
        GROUP BY 1;

--COUNT FCM CLINICS;
CREATE TABLE fcm_agg AS
    SELECT "index",
        SUBSTRING(fcm_clinic_geocode2."FIPS_BLK_GEO" FROM 1 FOR 11) 
            AS "FIPS_TRACT_GEO"
        FROM fcm_clinic_geocode2;

CREATE TABLE fcm_agg2 AS
    SELECT "FIPS_TRACT_GEO",
        COUNT("index") AS tct_total_fcm_clin_n
        FROM fcm_agg
        GROUP BY 1;

---MERGE RESOURCE DATA;
CREATE TABLE clin_resources3 AS
    SELECT acs_tract.tract_data_fin2."FIPS_TRACT_GEO", 
        wic_agg2.tct_total_wic_clin_n, 
        bbo_agg2.tct_total_bbo_clin_n, 
        fcm_agg2.tct_total_fcm_clin_n
        FROM acs_tract.tract_data_fin2
        LEFT JOIN wic_agg2 USING ("FIPS_TRACT_GEO")
        LEFT JOIN bbo_agg2 USING ("FIPS_TRACT_GEO")
        LEFT JOIN fcm_agg2 USING ("FIPS_TRACT_GEO");
