/*
This script joins block and tract FIPS to individual-level dataset 
Written by: Rashida Brown

*/

CREATE TABLE core_birth_info_rc AS
    SELECT core_birth_info_rc2.*, 
        indiv_census_tracts."FIPS_BLK_GEO",
        substring(indiv_census_tracts."FIPS_BLK_GEO" from 1 for 11) 
            AS "FIPS_TRACT_GEO"
        FROM core_birth_info_rc2
        LEFT JOIN indiv_census_tracts USING (latitude, longitude);

DROP TABLE core_birth_info_rc2;
