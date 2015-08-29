CREATE TABLE core_birth_info AS
    SELECT core_birth_info2.*,
        FLOOR((assess711_qmat."711_LT_D" - core_birth_info2."LMP_D")/7)
            AS "PREG_WKS_711_N",
        FLOOR((assess707g_qmat."707G_LT_D" - core_birth_info2."LMP_D")/7)
            AS "PREG_WKS_707G_N",
        assess711_qmat.*,
        assess707g_qmat.*
        FROM core_birth_info2
        LEFT JOIN assess711_qmat 
            ON (core_birth_info2."UNI_PART_ID_I" 
                    = assess711_qmat.unique_index_711)
        LEFT JOIN assess707g_qmat
            ON (core_birth_info2."UNI_PART_ID_I" 
                = assess707g_qmat.unique_index_707g);

CREATE TABLE core_birth_info_rc2 AS
    SELECT core_birth_info_rc1.*,
        FLOOR((assess711_qmat."711_LT_D" - core_birth_info_rc1."LMP_D")/7)
            AS "PREG_WKS_711_N",
        FLOOR((assess707g_qmat."707G_LT_D" - core_birth_info_rc1."LMP_D")/7)
            AS "PREG_WKS_707G_N",
        assess711_qmat.*,
        assess707g_qmat.*
        FROM core_birth_info_rc1
        LEFT JOIN assess711_qmat 
            ON (core_birth_info_rc1."UNI_PART_ID_I" 
                    = assess711_qmat.unique_index_711)
        LEFT JOIN assess707g_qmat
            ON (core_birth_info_rc1."UNI_PART_ID_I" 
                = assess707g_qmat.unique_index_707g);

DROP TABLE core_birth_info2, core_birth_info_rc1;

ALTER TABLE core_birth_info 
    DROP COLUMN unique_index_711,
    DROP COLUMN unique_index_707g;

ALTER TABLE core_birth_info_rc
    DROP COLUMN unique_index_711,
    DROP COLUMN unique_index_707g;

    