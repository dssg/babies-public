/*
This script appends county name and geocoded coordinates to the individual dataset.
Written by: Rashida Brown

*/

 CREATE TABLE core_birth_info2 AS
    WITH filter AS
    (
        SELECT core_birth_info1.*, 
            add_geocode_fin.add_geo2, 
            add_geocode_fin.confidence,
            add_geocode_fin.country_code3,
            add_geocode_fin.fips_county, 
            add_geocode_fin.latitude, 
            add_geocode_fin.locality, 
            add_geocode_fin.longitude, 
            add_geocode_fin.region
            FROM core_birth_info1
            LEFT JOIN add_geocode_fin USING ("PART_ID_I", "INFNT_ID_I")
    )
    SELECT filter.*,
        counties."CNTY_NAME_C"
        FROM filter
        LEFT JOIN counties USING (fips_county);

DROP TABLE core_birth_info1;

