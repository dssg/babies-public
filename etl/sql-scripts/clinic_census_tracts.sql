/*
This script formats FIPS codes and joins all public clinic data files address data
Written by: Rashida Brown

*/

CREATE TABLE wic_add_geocode2 AS
    SELECT wic_geocode_fin.*,
        wic_census_tracts."FIPS_BLK_GEO",
        substring(wic_census_tracts."FIPS_BLK_GEO" from 1 for 11)
            AS "FIPS_TRACT_GEO"
        FROM wic_geocode_fin
        LEFT JOIN wic_census_tracts USING (latitude, longitude);

CREATE TABLE fcm_clinic_geocode2 AS
    SELECT fcm_geocode_fin.*,
        fcm_census_tracts."FIPS_BLK_GEO",
        substring(fcm_census_tracts."FIPS_BLK_GEO" from 1 for 11)
            AS "FIPS_TRACT_GEO"
        FROM fcm_geocode_fin
        LEFT JOIN fcm_census_tracts USING (latitude, longitude);

CREATE TABLE bbo_clinic_geocode2 AS
    SELECT bbo_clinic_geocode_fin.*,
        bbo_census_tracts."FIPS_BLK_GEO",
        substring(bbo_census_tracts."FIPS_BLK_GEO" from 1 for 11)
            AS "FIPS_TRACT_GEO"
        FROM bbo_clinic_geocode_fin
        LEFT JOIN bbo_census_tracts USING (latitude, longitude);
