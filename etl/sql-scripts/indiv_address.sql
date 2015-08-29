/*
This script formats individual-level address data. Addresses containing PO boxes were not geocoded. 
Written by: Rashida Brown

*/

CREATE TABLE add_geocode_prep AS
    WITH filter AS
        (
            SELECT *,
                replace(to_char("ADDR_ZIP_N", '99999'), ' ', '') 
                    AS "ADDR_ZIP_N_cl"
                FROM
                (
                    SELECT "PART_ID_I",
                            "INFNT_ID_I",
                            "ADDR_LN1_T",
                            "ADDR_CTY_T", 
                            "ADDR_ST_C",
                            "ADDR_ZIP_N"
                            FROM core_birth_info
                ) AS tmp
        )
        SELECT *,
            CASE WHEN length("ADDR_ZIP_N_cl") < 5 
                THEN (rtrim("ADDR_LN1_T", ' ') || ', ' || 
                    rtrim("ADDR_CTY_T", ' ') || ', ' || 
                    rtrim("ADDR_ST_C", ' '))
                WHEN length("ADDR_ZIP_N_cl") = 5
                THEN (rtrim("ADDR_LN1_T", ' ') || ', ' || 
                    rtrim("ADDR_CTY_T", ' ') || ', ' || 
                    rtrim("ADDR_ST_C", ' ') || ', ' ||
                    rtrim("ADDR_ZIP_N_cl"))
                END AS add_geo2
            FROM filter WHERE "ADDR_LN1_T" NOT LIKE '%PO BOX%';
