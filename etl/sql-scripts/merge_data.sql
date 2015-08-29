/*
This script merges select information from the 'prenatal', 'postpart', 
'birth', 'catghist', 'partenroll' and 'address2' datasets into a single datasets
in order to quantify low birth weight and preterm births by geographic location 
and year.

Written by: Ian Pan, Rashida Brown, Laura Nolan

Date created: 06/17/2015
Date updated: 07/14/2015  
*/

-- get distinct pregnancies from prenatal
-- CREATE TABLE mergetemp1 AS
--  SELECT * FROM 
--  (
--      WITH tmp AS 
--      (
--          SELECT DISTINCT "PART_ID_I", "EDC_D", "LMP_D"
--              FROM prenatal ORDER BY "PART_ID_I"
--      )
--      SELECT *, ROW_NUMBER() OVER(PARTITION BY "PART_ID_I", "EDC_D"
--                                  ORDER BY "VISIT_D" DESC) AS rn
--          FROM tmp
--  ) AS tmp2 WHERE rn = 1;


CREATE TABLE mergetemp1 AS
    SELECT * FROM
        (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY "PART_ID_I", "EDC_D"
                                         ORDER BY gest_est) AS rn
                FROM 
                (
                    SELECT DISTINCT "PART_ID_I", "EDC_D", "LMP_D", 
                        @ ("EDC_D" - "LMP_D") - 280 
                            AS gest_est
                        FROM prenatal
                ) AS tmp
        ) AS tmp2 WHERE rn = 1;

ALTER TABLE mergetemp1
    DROP COLUMN rn;
-- merge with postpart, birth, address2, partenroll
-- get duplicate flag from partenroll
CREATE TABLE mergetemp2 AS
    SELECT *, 
        CASE WHEN "PART_ID_I" != "MOTHR_ID_I" AND "MOTHR_ID_I" NOT LIKE ' %'
            THEN 1 ELSE 0 END AS mismatch,
        FLOOR(("ACT_DLV_D" - "MOM_BTH_D") / 365) AS "DLV_AGE_N",
        FLOOR(("LMP_D" - "MOM_BTH_D") / 365) AS "CNCPT_AGE_N"
        FROM 
    (
        SELECT tmp.*, 

            public.birth."MOTHR_ID_I",
            public.birth."INF_WICM_F",
            public.birth."WGT_GRM_N",
            public.birth."BRTHCNTY_C",
            public.birth."ICU_F",
            CASE WHEN public.birth."ICU_F" = 'Y' 
                OR public.birth."INF_DISP_C" IN ('DD04', 'DD05') THEN 1 
                WHEN public.birth."ICU_F" = 'N' THEN 0
                ELSE NULL END AS "NICU_OTC",
            public.birth."INF_CMP1_C",
            public.birth."INF_CMP2_C",
            public.birth."INF_CMP3_C",
            public.birth."INF_CMP4_C",
            public.birth."INF_CMP5_C",
            public.birth."APORS_F",
            public.birth."INF_DISP_C",
            public.birth."DTH_CAUS_C",

            public.partenroll."MED_RISK_F" AS "MED_RISK2_F"

            FROM 
        (
            SELECT public.mergetemp1.*,

                public.postpart."INFNT_ID_I",
                public.postpart."VISIT_D" AS "POSTPART_VISIT_D",
                public.postpart."MALE_NBR_N",
                public.postpart."FEML_NBR_N",
                public.postpart."WGT_PNDS_N" AS "ENDPREG_WT_N",
                public.postpart."FETUSES_N", -- LM only p. 190
                public.postpart."LIV_BRTH_N", -- LM only
                public.postpart."LIV_POST_N", -- LM only
                public.postpart."WKS_DIED_N", -- LM only
                public.postpart."DLV_PLC_C",
                public.postpart."DLV_MTHD_C",
                public.postpart."FAM_PLAN_C",
                public.postpart."PREG_WKS_N", 
                public.postpart."PREG_OTC_C",
                public.postpart."STILLBRN_N", -- LM only
                public.postpart."LW_BRTH_N",
                public.postpart."PT_BRTH_N",
                public.postpart."RE_HOSP_F",
                public.postpart."PREG_HSP_N",
                public.postpart."CMPL_CD1_C",
                public.postpart."CMPL_CD2_C",
                public.postpart."CMPL_CD3_C",
                public.postpart."CMPL_CD4_C",
                public.postpart."CMPL_CD5_C",
                public.postpart."PMD_VSIT_N",
                public.postpart."SMK3_MTH_F" AS "POST_SMK_TRI3_F",
                public.postpart."CIG3_DAY_N" AS "POST_CIG3_DAY_N",
                public.postpart."DRK3_MTH_F" AS "POST_DRK_TRI3_F",
                public.postpart."DR_DY_WK_N" AS "POST_DR_DY_WK_N",
                public.postpart."DRNK_DAY_N" AS "POST_DRNK_DAY_N",
                public.postpart."ACT_DLV_D",

                public.address2."ADDR_LN1_T",
                public.address2."ADDR_LN2_T",
                public.address2."ADDR_CTY_T", 
                public.address2."ADDR_ST_C",
                public.address2."ADDR_ZIP_N",
                public.address2."ZIP_EXT_N", 
                public.address2."COUNTY_C",
                public.address2."RELATE_C",
                CASE WHEN public.address2."RELATE_C" = '    ' 
                    THEN 0 ELSE 1 END AS "SOC_SUPPORT_F",

                public.partenroll."MED_RISK_F",
                public.partenroll."BIRTH_D" AS "MOM_BTH_D",
                public.partenroll."EDUCATN_C",
                public.partenroll."EMPLYMNT_C",
                public.partenroll."RACE_C",
                public.partenroll."ETHNIC_C",
                public.partenroll."MRT_STAT_C",
                public.partenroll."RES_STAT_C",
                public.partenroll."DSBLTY_1_C",
                public.partenroll."LANG_1_C",
                public.partenroll."PA_CDE1_C"

                FROM mergetemp1 
                INNER JOIN postpart 
                    ON mergetemp1."PART_ID_I" = postpart."PART_ID_I"
                        AND mergetemp1."EDC_D" = postpart."PREN_EDC_D"
                LEFT JOIN address2 
                    ON mergetemp1."PART_ID_I" = address2."ADDR_ID_I"
                LEFT JOIN partenroll 
                    ON mergetemp1."PART_ID_I" = partenroll."PART_ID_I"
        ) AS tmp

        INNER JOIN birth
            ON tmp."INFNT_ID_I" = birth."PART_ID_I"
        LEFT JOIN partenroll 
            ON tmp."INFNT_ID_I" = partenroll."PART_ID_I"

    ) AS tmp2
        WHERE ("MED_RISK_F" != 'D' OR "MED_RISK_F" IS NULL)
            AND ("MED_RISK2_F" != 'D' OR "MED_RISK2_F" IS NULL)
            AND "ADDR_LN1_T" NOT LIKE '%DUPLI%' 
            AND "ADDR_LN2_T" NOT LIKE '%DUPLI%' 
            AND "PREG_OTC_C" IN ('LS  ', 'LM  ', 'ND  ', 'SM  ')
        ORDER BY "PART_ID_I", "ACT_DLV_D";

DROP TABLE mergetemp1;

CREATE TABLE mergetemp3 AS
    SELECT * FROM
    (
        SELECT *, 
            ROW_NUMBER() OVER (PARTITION BY "PART_ID_I", "ACT_DLV_D"
                               ORDER BY "MOTHR_ID_I" DESC,
                                   "INF_WICM_F" DESC,
                                   "FAM_PLAN_C" DESC,
                                   "PMD_VSIT_N" DESC) 
                AS rn2
            FROM 
            (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY "INFNT_ID_I"
                                       ORDER BY mismatch,
                                           "FAM_PLAN_C" DESC,
                                           "PMD_VSIT_N" DESC) AS rn
                    FROM mergetemp2
            ) AS tmp WHERE rn = 1
    ) AS tmp2 WHERE rn2 = 1;

DROP TABLE mergetemp2;

ALTER TABLE mergetemp3
    DROP COLUMN rn,
    DROP COLUMN rn2;
/*
more duplicate filtering to get rid of instances where mother gives birth to
different children on the same day 

partition by mother ID and delivery date and order by whether mother is WIC
and by baby's birth weight

per meeting with IDHS on 6/18/2015, Barbara said that WIC information is more
accurate, so if one duplicate record has WIC mother indicator as Yes and the 
other as No, take the one with Yes

if WIC indicator is identical between the records, then take the one with the 
lower birthweight (sometimes a duplicate record contains 9999 as birth weight),
if birthweight is the same, take the lower pregnancy weight (same rationale)

also partition by mother and infant IDs and take the earlier postpart visit
because a lot of the duplicates I was seeing had "DUPLICATE" in some address
field for the later postpart visit date

otherwise, it's random
*/

-- create outcome variables
CREATE TABLE mergetemp4 AS
    SELECT *,

        CASE WHEN "PREG_WKS_N" < 20 OR "PREG_WKS_N" > 44
            AND ("PREG_OTC_C" NOT LIKE 'LM%' OR "PREG_OTC_C" NOT LIKE 'LS%')
            THEN NULL
            WHEN "PREG_WKS_N" < 37 
            THEN 1
            WHEN "PREG_WKS_N" >= 37 
            THEN 0
            END AS "PTB_OTC",

        CASE WHEN "PREG_WKS_N" < 20 OR "PREG_WKS_N" > 44
            AND ("PREG_OTC_C" NOT LIKE 'LM%' OR "PREG_OTC_C" NOT LIKE 'LS%')
            THEN NULL
            WHEN "PREG_WKS_N" < 32 
            THEN 1
            WHEN "PREG_WKS_N" >= 32 
            THEN 0
            END AS "VPTB_OTC",

        CASE WHEN "WGT_GRM_N" = 9999 OR "PREG_WKS_N" < 20 OR "PREG_WKS_N" > 44
            AND ("PREG_OTC_C" NOT LIKE 'LM%' OR "PREG_OTC_C" NOT LIKE 'LS%')
            THEN NULL
            WHEN "WGT_GRM_N" < 2500 
            THEN 1
            WHEN "WGT_GRM_N" >= 2500 
            THEN 0
            END AS "LBW_OTC",

        CASE WHEN "WGT_GRM_N" = 9999 OR "PREG_WKS_N" < 20 OR "PREG_WKS_N" > 44
            AND ("PREG_OTC_C" NOT LIKE 'LM%' OR "PREG_OTC_C" NOT LIKE 'LS%')
            THEN NULL
            WHEN "WGT_GRM_N" < 1500 
            THEN 1
            WHEN "WGT_GRM_N" >= 1500 
            THEN 0
            END AS "VLBW_OTC",

        CASE WHEN "WKS_DIED_N" = 99 OR "PREG_WKS_N" < 20 OR "PREG_WKS_N" > 44
            AND ("PREG_OTC_C" NOT LIKE 'LM%' OR "PREG_OTC_C" NOT LIKE 'LS%')
            THEN NULL
            WHEN "WKS_DIED_N" > 0 OR "STILLBRN_N" > 0 
            OR "PREG_OTC_C" IN ('SM%', 'ND%') OR "INF_DISP_C" = 'DD01'
            OR "DTH_CAUS_C" NOT LIKE ' %'
            THEN 1
            WHEN "WKS_DIED_N" = 0 
            THEN 0
            END AS "INFM_OTC",

        CASE WHEN "CMPL_CD1_C" = '    ' AND "CMPL_CD2_C" = '    ' 
            AND "CMPL_CD3_C" = '    ' AND "CMPL_CD4_C" = '    '
            AND "CMPL_CD5_C" = '    ' 
            THEN 0
            WHEN "CMPL_CD1_C" != '    ' OR "CMPL_CD2_C" != '    ' 
            OR "CMPL_CD3_C" != '    ' OR "CMPL_CD4_C" != '    ' 
            OR "CMPL_CD5_C" != '    ' 
            THEN 1
            ELSE NULL
            END AS "PREG_COMPL_OTC",

        CASE WHEN "INF_CMP1_C" IN ('    ', '10  ') 
            AND "INF_CMP2_C" IN ('    ', '10  ')
            AND "INF_CMP3_C" IN ('    ', '10  ') 
            AND "INF_CMP4_C" IN ('    ', '10  ')
            AND "INF_CMP5_C" IN ('    ', '10  ') 
            THEN 0
            WHEN "INF_CMP1_C" NOT IN ('    ', '10  ') 
            OR "INF_CMP2_C" NOT IN ('    ', '10  ') 
            OR "INF_CMP3_C" NOT IN ('    ', '10  ') 
            OR "INF_CMP4_C" NOT IN ('    ', '10  ')
            OR "INF_CMP5_C" NOT IN ('    ', '10  ')
            THEN 1 
            ELSE NULL
            END AS "INF_COMPL_OTC",   

        to_number(
            substring(to_char("ACT_DLV_D", 'YYYY/MM/DD') FROM 1 FOR 4), 
            '9999D'
        ) AS "ACT_DLV_YR_C"

    FROM mergetemp3;

DROP TABLE mergetemp3;

-- create aggregate adverse birth outcome
CREATE TABLE mergetemp5 AS
    SELECT *,
        CASE WHEN "PTB_OTC" = 1 OR "LBW_OTC" = 1 OR "INFM_OTC" = 1
            OR "PREG_COMPL_OTC" = 1 OR "INF_COMPL_OTC" = 1 OR "NICU_OTC" = 1
            THEN 1
            WHEN "PTB_OTC" = 0 AND "LBW_OTC" = 0 AND "INFM_OTC" = 0
            AND "PREG_COMPL_OTC" = 0 AND "INF_COMPL_OTC" = 0 AND "NICU_OTC" = 0
            THEN 0
            ELSE NULL
            END AS "ADVB_OTC",
        CASE WHEN "VPTB_OTC" = 1 OR "VLBW_OTC" = 1 OR "INFM_OTC" = 1
            OR "NICU_OTC" = 1
            THEN 1
            WHEN "VPTB_OTC" = 0 AND "VLBW_OTC" = 0 AND "INFM_OTC" = 0
            AND "NICU_OTC" = 0
            THEN 0
            ELSE NULL
            END AS "VADVB_OTC"
    FROM mergetemp4;

DROP TABLE mergetemp4;

/*
1 create table with FCM (pregnant) and IPCM history
2 effective period must be at least 30 days
3 merge with birth cohort 
4 calculate an enrollment number
*/
CREATE TABLE cm_history AS
    
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY "PART_ID_I", "ACT_DLV_D", "CATEGORY_C"
                           ORDER BY "PART_ID_I", "EFF_FROM_D") AS "ENRL_N" 
        FROM 
        (
        -- calculate enrollment time for each row (FCM/IPCM only)
        WITH filter AS
        (
            SELECT *, 
                "EFF_THRU_D" - "EFF_FROM_D" AS "ENROLL_TIME_N" 
                FROM catghist
                WHERE "PGM_ID_C" = 'CM' AND "CATEGORY_C" IN ('P', 'IPCM')
        ) 
        -- extract only rows with enrollment at least 30 days
        SELECT filter.*,
            mergetemp5."LMP_D",
            mergetemp5."ACT_DLV_D"
            FROM filter
            INNER JOIN mergetemp5
                ON filter."PART_ID_I" = mergetemp5."PART_ID_I"
            WHERE filter."EFF_FROM_D" BETWEEN "LMP_D" AND "ACT_DLV_D" AND 
                ("ENROLL_TIME_N" >= 30 OR filter."EFF_THRU_D" IS NULL)
        ) AS tmp;


-- calculate difference between previous effective through 
-- and next effective from
CREATE TABLE cm_history2 AS 
    WITH measure AS
    (
        WITH filter AS
        (
            SELECT *, 
                ROW_NUMBER() OVER (ORDER BY "PART_ID_I", "ENRL_N") AS rn 
                FROM cm_history
        )
        SELECT "PART_ID_I",
            "ACT_DLV_D",
            "CATEGORY_C",
            "ENRL_N" - 1 AS "ENRLX_N",
            "EFF_FROM_D" AS "NEXT_START_D"
            FROM filter
    )
    SELECT cm_history.*,
        measure."NEXT_START_D",
        measure."NEXT_START_D" - cm_history."EFF_THRU_D" AS "GAP_ENRL_N"
        FROM cm_history
        LEFT JOIN measure 
            ON cm_history."PART_ID_I" = measure."PART_ID_I"
                AND cm_history."CATEGORY_C" = measure."CATEGORY_C"
                AND cm_history."ACT_DLV_D" = measure."ACT_DLV_D"
                AND cm_history."ENRL_N" = measure."ENRLX_N"; 

DROP TABLE cm_history;

CREATE TABLE cm_history3 AS
    WITH filter2 AS 
    (
        WITH filter AS
        (
            SELECT * FROM cm_history2 
                WHERE "GAP_ENRL_N" <= 7 
                    OR "GAP_ENRL_N" IS NULL
        )
        SELECT *,
            MIN("EFF_FROM_D") OVER (PARTITION BY "PART_ID_I", "ACT_DLV_D", 
                                                 "CATEGORY_C") 
                AS "START_D",
            MAX("EFF_THRU_D") OVER (PARTITION BY "PART_ID_I", "ACT_DLV_D",
                                                 "CATEGORY_C") 
                AS "END_D"
            FROM filter 
    )
    SELECT *, "END_D" - "START_D" AS "ENROLLX_TIME_N" FROM filter2;

DROP TABLE cm_history2;

CREATE TABLE mergetemp6 AS
    WITH ipcm_hist AS
    (
        SELECT DISTINCT "PART_ID_I",
            "ACT_DLV_D",
            "CATEGORY_C",
            "START_D",
            "END_D",
            "ENROLLX_TIME_N"
            FROM cm_history3 
            WHERE "CATEGORY_C" = 'IPCM'
    ),
    fcm_hist AS
    (
        SELECT DISTINCT "PART_ID_I",
            "ACT_DLV_D",
            "CATEGORY_C",
            "START_D",
            "END_D",
            "ENROLLX_TIME_N"
            FROM cm_history3 
            WHERE "CATEGORY_C" = 'P'
    )
    SELECT mergetemp5.*,
        ipcm_hist."CATEGORY_C" AS "IPCM_CAT_C",
        ipcm_hist."START_D" AS "IPCM_START_D",
        ipcm_hist."END_D" AS "IPCM_END_D",
        ipcm_hist."ENROLLX_TIME_N" AS "IPCM_ENRL_TIME_N",
        fcm_hist."CATEGORY_C" AS "FCM_CAT_C",
        fcm_hist."START_D" AS "FCM_START_D",
        fcm_hist."END_D" AS "FCM_END_D",
        fcm_hist."ENROLLX_TIME_N" AS "FCM_ENRL_TIME_N"
        FROM mergetemp5
        LEFT JOIN ipcm_hist
            ON mergetemp5."PART_ID_I" = ipcm_hist."PART_ID_I"
                AND mergetemp5."ACT_DLV_D" = ipcm_hist."ACT_DLV_D"
        LEFT JOIN fcm_hist
            ON mergetemp5."PART_ID_I" = fcm_hist."PART_ID_I"
                AND mergetemp5."ACT_DLV_D" = fcm_hist."ACT_DLV_D";

DROP TABLE mergetemp5;

CREATE TABLE mergetemp7 AS
    SELECT *,
        CASE WHEN "IPCM_CAT_C" IS NOT NULL THEN 1 ELSE 0 END AS "BBO_F",
        CASE WHEN "FCM_CAT_C" IS NOT NULL THEN 1 ELSE 0 END AS "FCM_F"
        FROM mergetemp6;

DROP TABLE mergetemp6, cm_history3;

CREATE TABLE cs_birth_temp AS
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY "PART_ID_I" 
                           ORDER BY "ACT_DLV_D")
            AS "CS_BIRTH_N"
        FROM mergetemp7
        ORDER BY 1,2;

CREATE TABLE cs_birth_temp2 AS 
    WITH filter2 AS 
        (
        WITH filter AS 
        (
            SELECT *,
                "CS_BIRTH_N" + 1 AS "CSX_BIRTH_N"
                FROM cs_birth_temp
        )
        SELECT cs_birth_temp.*,
            filter."ACT_DLV_D" AS "PREV_DLV_D"
            FROM cs_birth_temp
            LEFT JOIN filter
                ON cs_birth_temp."PART_ID_I" = filter."PART_ID_I"
                    AND cs_birth_temp."CS_BIRTH_N" = filter."CSX_BIRTH_N"
    )
    SELECT *,
        "ACT_DLV_D" - "PREV_DLV_D" AS "TIME_BTWN_PREG_N",
        "ACT_DLV_D" - "LMP_D" AS "GEST_N"
        FROM filter2;

CREATE TABLE cs_birth_temp3 AS
    WITH filter AS
    (
        SELECT * FROM cs_birth_temp2 WHERE "TIME_BTWN_PREG_N" < "GEST_N"
    )
    SELECT cs_birth_temp2.*
        FROM cs_birth_temp2 
        INNER JOIN filter 
            ON cs_birth_temp2."PART_ID_I" = filter."PART_ID_I" 
            AND (cs_birth_temp2."CS_BIRTH_N" = filter."CS_BIRTH_N" 
                OR cs_birth_temp2."CS_BIRTH_N" = filter."CS_BIRTH_N"-1);

CREATE TABLE cs_birth_temp4 AS
    SELECT * FROM 
    (
        SELECT *, 
            ROW_NUMBER() OVER (PARTITION BY "PART_ID_I" 
                               ORDER BY "MOTHR_ID_I" DESC, 
                                   "INF_WICM_F" DESC, 
                                   "FAM_PLAN_C" DESC, 
                                   "PMD_VSIT_N" DESC)
                AS rn 
            FROM cs_birth_temp3
    ) AS tmp WHERE rn = 1;


ALTER TABLE cs_birth_temp4
    DROP COLUMN rn;

DROP TABLE cs_birth_temp;

CREATE TABLE mergetemp8 AS
    SELECT * FROM cs_birth_temp2
    EXCEPT
    (
        SELECT * FROM cs_birth_temp3
        EXCEPT
        SELECT * FROM cs_birth_temp4
    );

ALTER TABLE mergetemp8 
    DROP COLUMN "CS_BIRTH_N";

DROP TABLE mergetemp7, cs_birth_temp2, cs_birth_temp3, cs_birth_temp4;

CREATE TABLE mergetemp9 AS
    SELECT *, concat("PART_ID_I", '_', "CS_BIRTH_N") AS "UNI_PART_ID_I"
        FROM 
        (
        SELECT *, 
            ROW_NUMBER() OVER (PARTITION BY "PART_ID_I" ORDER BY "ACT_DLV_D") 
                AS "CS_BIRTH_N"
            FROM mergetemp8
        ) AS tmp;

DROP TABLE mergetemp8;
/*
This script condenses the information in the prenatal and hlthvsit datasets
so that there is one feature vector per birth, rather than information for 
multiple visits during the same pregnancy.

Written by: Ian Pan, Laura Nolan, Rashida Brown
Date created: 06/26/2015
Date updated: 07/06/2015
*/

-- merge birth cohort with prenatal
CREATE TABLE prenatal_merge AS
    SELECT prenatal.*,
        mergetemp9."ACT_DLV_D"
        FROM prenatal
        INNER JOIN mergetemp9
            ON prenatal."PART_ID_I" = mergetemp9."PART_ID_I"
                AND prenatal."EDC_D" = mergetemp9."EDC_D";

-- recalculate and extract number of pregnancies, previous live births
CREATE TABLE mergetemp10 AS

    WITH filter AS 

    (

        SELECT *,
            CASE WHEN "XPREG_NBR_N" - "XLV_BRTH_N" <= 0 THEN "XLV_BRTH_N" + 1
                ELSE "XPREG_NBR_N" END AS "TPREG_NBR_N"
            FROM 

        (
            SELECT *,
                MAX("TRUE_PREG_NBR_N") OVER 
                	(PARTITION BY "PART_ID_I", "ACT_DLV_D") 
                    AS "XPREG_NBR_N",
                MAX("TRUE_LV_BRTH_N") OVER 
                    (PARTITION BY "PART_ID_I", "ACT_DLV_D")
                    AS "XLV_BRTH_N",
                MAX("PRI_VSIT_N") OVER
                	(PARTITION BY "PART_ID_I", "ACT_DLV_D")
                	AS "XPRI_VSIT_N"
                FROM 

            (
                SELECT *,
                    CASE WHEN "PREG_NBR_N" >= 10 THEN NULL 
                        ELSE "PREG_NBR_N" END
                        AS "TRUE_PREG_NBR_N",
                    CASE WHEN "LV_BRTH_N" >= 10 THEN NULL 
                        ELSE "LV_BRTH_N" END
                        AS "TRUE_LV_BRTH_N"
                    FROM prenatal_merge

            ) AS tmp 

        ) AS tmp2

    )

    SELECT DISTINCT * FROM
    (
        SELECT mergetemp9.*,
            filter."TPREG_NBR_N" AS "XPREG_NBR_N",
            filter."XLV_BRTH_N",
            filter."XPRI_VSIT_N"
            FROM mergetemp9
            LEFT JOIN filter
                ON mergetemp9."PART_ID_I" = filter."PART_ID_I"
                    AND mergetemp9."EDC_D" = filter."EDC_D"
    ) AS tmp3;

DROP TABLE mergetemp9;

-- extract last live birth date and birth weight, month care began
CREATE TABLE mergetemp11 AS
    WITH filter AS
    (
        SELECT * FROM 
        (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY "PART_ID_I", "ACT_DLV_D"
                                         ORDER BY "LSTLV_BR_D" DESC) AS rn
                FROM 
                (
                    SELECT "PART_ID_I", "ACT_DLV_D", "LSTLV_BR_D", "LST_WGTG_N"
                        FROM prenatal_merge 
                        WHERE "ACT_DLV_D" - "LSTLV_BR_D" BETWEEN 280 AND 10000
                ) AS tmp
        ) AS tmp2 WHERE rn = 1
    )
    SELECT mergetemp10.*,
        filter."LSTLV_BR_D",
        filter."LST_WGTG_N"
        FROM mergetemp10
        LEFT JOIN filter
            ON mergetemp10."PART_ID_I" = filter."PART_ID_I"
                AND mergetemp10."ACT_DLV_D" = filter."ACT_DLV_D";

DROP TABLE mergetemp10;

-- extract earliest month care began
CREATE TABLE mergetemp12 AS
    WITH filter AS
    (
        SELECT * FROM 
        (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY "PART_ID_I", "ACT_DLV_D" 
                                         ORDER BY "XMONCARE_N") AS rn
                FROM 
                (
                    SELECT *,
                        CASE WHEN "MONCARE_N" = 0 THEN 8.5 
                            ELSE "MONCARE_N" END AS "XMONCARE_N"
                        FROM prenatal_merge
                ) AS tmp
        ) AS tmp2 WHERE rn = 1
    )
    SELECT mergetemp11.*,
        CASE WHEN filter."XMONCARE_N" = 8.5 THEN 0 
            ELSE filter."XMONCARE_N" END AS "XMONCARE_N"
        FROM mergetemp11
        LEFT JOIN filter
            ON mergetemp11."PART_ID_I" = filter."PART_ID_I"
                AND mergetemp11."ACT_DLV_D" = filter."ACT_DLV_D";

DROP TABLE mergetemp11;

-- extract pre-pregnancy height/weight, filter out unreasonable values 
CREATE TABLE mergetemp13 AS
    WITH filter AS 
    (
        SELECT * FROM 
        (
            SELECT "PART_ID_I", "VISIT_D", "ACT_DLV_D", 
                "WGT_PNDS_N", "WGT_OZS_N",
                "HGT_FEET_N", "HGT_INCH_N", "HGT_QICH_N",
                "SMK3_MTH_F" AS "PRE_SMK3_MTH_F",
                "CIG3_DAY_N" AS "PRE_CIG3_DAY_N",
                "DRK3_MTH_F" AS "PRE_DRK3_MTH_F",
                "DR_DY_WK_N" AS "PRE_DR_DY_WK_N",
                "DRNK_DAY_N" AS "PRE_DRNK_DAY_N",
                ROW_NUMBER() OVER (PARTITION BY "PART_ID_I", "ACT_DLV_D"
                                   ORDER BY "VISIT_D") AS rn
                FROM 
                (
                    SELECT * FROM prenatal_merge
                        WHERE "WGT_PNDS_N" BETWEEN 50 AND 900 
                            AND "WGT_OZS_N" != 99 
                            AND "HGT_FEET_N" BETWEEN 4 AND 6 
                            AND "HGT_INCH_N" != 99
                ) AS tmp
        ) AS tmp2 WHERE rn = 1
    )
    SELECT mergetemp12.*,
        filter."HGT_FEET_N" AS "PRE_HGT_FEET_N",
        filter."HGT_INCH_N" AS "PRE_HGT_INCH_N",
        filter."HGT_QICH_N" AS "PRE_HGT_QICH_N",
        filter."WGT_PNDS_N" AS "PRE_WGT_PNDS_N",
        filter."WGT_OZS_N" AS "PRE_WGT_OZS_N",
        filter."PRE_SMK3_MTH_F",
        filter."PRE_CIG3_DAY_N",
        filter."PRE_DRK3_MTH_F",
        filter."PRE_DR_DY_WK_N",
        filter."PRE_DRNK_DAY_N"
        FROM mergetemp12
        LEFT JOIN filter 
            ON mergetemp12."PART_ID_I" = filter."PART_ID_I"
                AND mergetemp12."ACT_DLV_D" = filter."ACT_DLV_D";

DROP TABLE mergetemp12;

-- compute weight gain, pre-/post-pregnancy BMIs
CREATE TABLE mergetemp14 AS
    WITH filter AS
    (
        SELECT "PART_ID_I", "ACT_DLV_D",
            "ENDPREG_WT_N" - ("PRE_WGT_PNDS_N" + "PRE_WGT_OZS_N" / 16) 
                AS "WGT_GAIN_N",
            ("PRE_WGT_PNDS_N" + ("PRE_WGT_OZS_N" / 16)) * 0.453592 
                AS pre_kg,
            ("PRE_HGT_FEET_N" + ("PRE_HGT_INCH_N" / 12) + 
                ("PRE_HGT_QICH_N" / 48)) * 0.3048 AS m,
            "ENDPREG_WT_N" * 0.453592 AS post_kg 
            FROM mergetemp13
    )
    SELECT mergetemp13.*,
        filter."WGT_GAIN_N",
        filter.pre_kg / power(filter.m, 2) AS "PRE_BMI_N",
        filter.post_kg / power(filter.m, 2) AS "POST_BMI_N"
        FROM mergetemp13 
        LEFT JOIN filter 
            ON mergetemp13."PART_ID_I" = filter."PART_ID_I"
                AND mergetemp13."ACT_DLV_D" = filter."ACT_DLV_D";

DROP TABLE mergetemp13, prenatal_merge; 

-- merge birth cohort with hlthvsit
CREATE TABLE hlthvsit_merge AS
    SELECT hlthvsit.*,
        mergetemp14."ACT_DLV_D",
        mergetemp14."LMP_D"
        FROM hlthvsit
        INNER JOIN mergetemp14
            ON hlthvsit."PART_ID_I" = mergetemp14."PART_ID_I"
        WHERE "VISIT_D" BETWEEN "LMP_D" AND "ACT_DLV_D";

-- determine trimester end dates
CREATE TABLE hlth_temp1 AS
    SELECT *,
        ("LMP_D" + INTERVAL '84' DAY)::date AS "TRI1_END_D",
        ("LMP_D" + INTERVAL '196' DAY)::date AS "TRI2_END_D"
        FROM hlthvsit_merge;

-- determine whether visit was recorded during tri1, tri2, or tri3
CREATE TABLE hlth_temp2 AS
    SELECT *, 
        CASE WHEN "VISIT_D" < "TRI1_END_D" THEN 1 ELSE 0 END
            AS "TRI1_VISIT_F",
        CASE WHEN "VISIT_D" >= "TRI1_END_D"
            AND "VISIT_D" < "TRI2_END_D" THEN 1 ELSE 0 END
            AS "TRI2_VISIT_F",
        CASE WHEN "VISIT_D" >= "TRI2_END_D" THEN 1 ELSE 0 END
            AS "TRI3_VISIT_F"
        FROM hlth_temp1 ORDER BY "PART_ID_I", "VISIT_D";

DROP TABLE hlth_temp1;

/*
determine whether woman
1) smoked
2) drank
3) had quit-smoking intervention
4) took prenatal vitamins
during each trimester; if no visit was recorded during a trimester, then NULL
*/
CREATE TABLE hlth_temp3 AS
    SELECT DISTINCT * FROM
    (
        SELECT "PART_ID_I", "ACT_DLV_D",

            CASE WHEN "CUR_SMK_F" = 'Y' 
                AND "TRI1_VISIT_F" = 1 THEN 1 
            WHEN "CUR_SMK_F" = 'N' 
                AND "TRI1_VISIT_F" = 1 THEN 0
                ELSE NULL END AS smoking_tri1,

            CASE WHEN "CUR_SMK_F" = 'Y' 
                AND "TRI2_VISIT_F" = 1 THEN 1
            WHEN "CUR_SMK_F" = 'N' 
                AND "TRI2_VISIT_F" = 1 THEN 0
                ELSE NULL END AS smoking_tri2,

            CASE WHEN "CUR_DRNK_F" = 'Y' 
                AND "TRI1_VISIT_F" = 1 THEN 1 
            WHEN "CUR_DRNK_F" = 'N' 
                AND "TRI1_VISIT_F" = 1 THEN 0
                ELSE NULL END AS drinking_tri1,

            CASE WHEN "CUR_DRNK_F" = 'Y' 
                AND "TRI2_VISIT_F" = 1 THEN 1
            WHEN "CUR_DRNK_F" = 'N' 
                AND "TRI2_VISIT_F" = 1 THEN 0
                ELSE NULL END AS drinking_tri2,

            CASE WHEN "INTERV_F" = 'Y'
                AND "TRI1_VISIT_F" = 1 THEN 1 
            WHEN "INTERV_F" = 'N'
                AND "TRI1_VISIT_F" = 1 THEN 0
                ELSE NULL END AS interv_tri1,

            CASE WHEN "INTERV_F" = 'Y'
                AND "TRI2_VISIT_F" = 1 THEN 1 
            WHEN "INTERV_F" = 'N'
                AND "TRI3_VISIT_F" = 1 THEN 0
                ELSE NULL END AS interv_tri2,

            CASE WHEN "INTERV_F" = 'Y'
                AND "TRI3_VISIT_F" = 1 THEN 1 
            WHEN "INTERV_F" = 'N'
                AND "TRI3_VISIT_F" = 1 THEN 0
                ELSE NULL END AS interv_tri3,

            CASE WHEN "VIT_MIN_C" IN ('02', '03', '04') 
                AND "TRI1_VISIT_F" = 1 THEN 1
            WHEN "VIT_MIN_C" IN ('01', '05') 
                AND "TRI1_VISIT_F" = 1 THEN 0
                ELSE NULL END AS vit_tri1,

            CASE WHEN "VIT_MIN_C" IN ('02', '03', '04') 
                AND "TRI2_VISIT_F" = 1 THEN 1
            WHEN "VIT_MIN_C" IN ('01', '05') 
                AND "TRI2_VISIT_F" = 1 THEN 0
                ELSE NULL END AS vit_tri2,

            CASE WHEN "VIT_MIN_C" IN ('02', '03', '04') 
                AND "TRI3_VISIT_F" = 1 THEN 1
                WHEN "VIT_MIN_C" IN ('01', '05') 
                AND "TRI3_VISIT_F" = 1 THEN 0
                ELSE NULL END AS vit_tri3

        FROM hlth_temp2
    ) AS tmp;

DROP TABLE hlth_temp2;

-- deal with duplicates by taking the sum over every pregnancy
-- create binary indicators from sums
CREATE TABLE hlth_temp4 AS
    WITH filter AS 
    (
        SELECT DISTINCT "PART_ID_I", "ACT_DLV_D",
            "SMK_TRI1_F", "SMK_TRI2_F",
            "DRK_TRI1_F", "DRK_TRI2_F",
            "INV_TRI1_F", "INV_TRI2_F", "INV_TRI3_F",
            "VIT_TRI1_F", "VIT_TRI2_F", "VIT_TRI3_F"
            FROM 
            (
                SELECT *, 
                    SUM(smoking_tri1) 
                        OVER (PARTITION BY "PART_ID_I", "ACT_DLV_D") 
                        AS "SMK_TRI1_F",
                    SUM(smoking_tri2) 
                        OVER (PARTITION BY "PART_ID_I", "ACT_DLV_D") 
                        AS "SMK_TRI2_F",
                    SUM(drinking_tri1) 
                        OVER (PARTITION BY "PART_ID_I", "ACT_DLV_D") 
                        AS "DRK_TRI1_F",
                    SUM(drinking_tri2) 
                        OVER (PARTITION BY "PART_ID_I", "ACT_DLV_D") 
                        AS "DRK_TRI2_F",
                    SUM(interv_tri1) OVER 
                        (PARTITION BY "PART_ID_I", "ACT_DLV_D") 
                        AS "INV_TRI1_F",
                    SUM(interv_tri2) OVER 
                        (PARTITION BY "PART_ID_I", "ACT_DLV_D") 
                        AS "INV_TRI2_F",
                    SUM(interv_tri3) 
                        OVER (PARTITION BY "PART_ID_I", "ACT_DLV_D") 
                        AS "INV_TRI3_F",
                    SUM(vit_tri1) OVER 
                        (PARTITION BY "PART_ID_I", "ACT_DLV_D") 
                        AS "VIT_TRI1_F",
                    SUM(vit_tri2) OVER 
                        (PARTITION BY "PART_ID_I", "ACT_DLV_D") 
                        AS "VIT_TRI2_F",
                    SUM(vit_tri3) OVER 
                        (PARTITION BY "PART_ID_I", "ACT_DLV_D") 
                        AS "VIT_TRI3_F"
                FROM hlth_temp3
            ) AS tmp
    )
    SELECT "PART_ID_I", "ACT_DLV_D",
        CASE WHEN "SMK_TRI1_F" >= 1 THEN 1 
            WHEN "SMK_TRI1_F" IS NULL THEN NULL ELSE 0 END AS "SMK_TRI1_F",
        CASE WHEN "SMK_TRI2_F" >= 1 THEN 1 
            WHEN "SMK_TRI2_F" IS NULL THEN NULL ELSE 0 END AS "SMK_TRI2_F",
        CASE WHEN "DRK_TRI1_F" >= 1 THEN 1 
            WHEN "DRK_TRI1_F" IS NULL THEN NULL ELSE 0 END AS "DRK_TRI1_F",
        CASE WHEN "DRK_TRI2_F" >= 1 THEN 1 
            WHEN "DRK_TRI2_F" IS NULL THEN NULL ELSE 0 END AS "DRK_TRI2_F",
        CASE WHEN "INV_TRI1_F" >= 1 THEN 1 
            WHEN "INV_TRI1_F" IS NULL THEN NULL ELSE 0 END AS "INV_TRI1_F",
        CASE WHEN "INV_TRI2_F" >= 1 THEN 1 
            WHEN "INV_TRI2_F" IS NULL THEN NULL ELSE 0 END AS "INV_TRI2_F",
        CASE WHEN "INV_TRI3_F" >= 1 THEN 1 
            WHEN "INV_TRI3_F" IS NULL THEN NULL ELSE 0 END AS "INV_TRI3_F",
        CASE WHEN "VIT_TRI1_F" >= 1 THEN 1
            WHEN "VIT_TRI1_F" IS NULL THEN NULL ELSE 0 END AS "VIT_TRI1_F",
        CASE WHEN "VIT_TRI2_F" >= 1 THEN 1
            WHEN "VIT_TRI2_F" IS NULL THEN NULL ELSE 0 END AS "VIT_TRI2_F",
        CASE WHEN "VIT_TRI3_F" >= 1 THEN 1
            WHEN "VIT_TRI3_F" IS NULL THEN NULL ELSE 0 END AS "VIT_TRI3_F"
        FROM filter;

DROP TABLE hlth_temp3;

CREATE TABLE income AS 
    SELECT "PART_ID_I", "PGM_ID_C", "STRTCERT_D", 
        "HSEHLD_N", "HSE_INC_A", "INC_PRF_C" 
        FROM program 
        WHERE "INC_PRF_C" NOT LIKE ' %' AND "INC_PRF_C" NOT LIKE '14%';

CREATE TABLE income2 AS
SELECT income.*, 
    mergetemp14."LMP_D"
    FROM income
    LEFT JOIN mergetemp14 USING ("PART_ID_I")
    WHERE "LMP_D" IS NOT NULL;

DROP TABLE income;

CREATE TABLE income3 AS
    SELECT * 
        FROM (
                SELECT *, 
                    ROW_NUMBER() OVER (PARTITION BY 
                                       "PART_ID_I", "LMP_D" 
                                       ORDER BY diff, "PGM_ID_C" DESC, 
                                           "HSE_INC_A" DESC) AS rn 
                    FROM (
                            SELECT *, 
                                abs("LMP_D"-"STRTCERT_D") 
                                AS diff 
                                FROM income2
                         ) AS tmp
             ) AS tmp2 WHERE rn = 1;

DROP TABLE income2; 

-- merge 
CREATE TABLE core_birth_info1 AS
    SELECT mergetemp14.*,
        hlth_temp4."SMK_TRI1_F",
        hlth_temp4."SMK_TRI2_F",
        hlth_temp4."DRK_TRI1_F",
        hlth_temp4."DRK_TRI2_F",
        hlth_temp4."INV_TRI1_F",
        hlth_temp4."INV_TRI2_F",
        hlth_temp4."INV_TRI3_F",
        hlth_temp4."VIT_TRI1_F",
        hlth_temp4."VIT_TRI2_F",
        hlth_temp4."VIT_TRI3_F"
        income3."HSEHLD_N",
        income3."HSE_INC_A",
        income3."INC_PRF_C",
        income3."STRTCERT_D",
        income3."PGM_ID_C" AS "INC_PGM_ID_C"
        FROM mergetemp14
        LEFT JOIN hlth_temp4 USING ("PART_ID_I", "ACT_DLV_D")
        LEFT JOIN income3 USING ("PART_ID_I", "LMP_D");

DROP TABLE mergetemp14, hlth_temp4, hlthvsit_merge, income3;