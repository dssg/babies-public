CREATE TABLE core_birth_info_rc1 AS

SELECT
    -- unique birth identifier 
    "UNI_PART_ID_I",
    -- date of last menstrual period
    "LMP_D",
    -- month of conception
    date_part('month', "LMP_D")::varchar(2) AS "MON_CNCPT_C",
    -- delivery date
    "ACT_DLV_D",
    -- delivery year
    "ACT_DLV_YR_C" AS "ACT_DLV_YR",
    -- age at delivery
    CASE WHEN "DLV_AGE_N" >= 15 
        THEN "DLV_AGE_N" ELSE NULL END AS "DLV_AGE_N",
    -- age at conception
    CASE WHEN "CNCPT_AGE_N" >= 15 
        THEN "CNCPT_AGE_N" ELSE NULL END AS "CNCPT_AGE_N",
    -- pre-pregnancy BMI
    CASE WHEN "PRE_BMI_N" BETWEEN 10 AND 60
        THEN "PRE_BMI_N" ELSE NULL END AS "PRE_BMI_N",
    -- post-pregnancy BMI
    CASE WHEN "POST_BMI_N" BETWEEN 10 AND 60
        THEN "POST_BMI_N" ELSE NULL END AS "POST_BMI_N",
    -- pre-pregnancy weight
    CASE WHEN "PRE_WGT_PNDS_N" BETWEEN 75 AND 350
        THEN "PRE_WGT_PNDS_N" ELSE NULL END AS "PREPREG_WT_N",
    -- post-pregnancy weight
    CASE WHEN "ENDPREG_WT_N" BETWEEN 75 AND 350
        THEN "ENDPREG_WT_N" ELSE NULL END AS "ENDPREG_WT_N",
    -- weight gain during pregnancy
    CASE WHEN "WGT_GAIN_N" BETWEEN -50 AND 100
        THEN "WGT_GAIN_N" ELSE NULL END AS "WGT_GAIN_N",
    -- race
    CASE WHEN "RACE_C" IN ('10  ', '20  ', '80  ', '50  ', '60  ', '70  ')
        THEN "RACE_C" ELSE NULL END AS "RACE_C",
    -- hispanic/latino
    CASE WHEN "ETHNIC_C" IN ('00  ', '01  ')
        THEN "ETHNIC_C" ELSE NULL END AS "ETHNIC_C",
    -- education level
    CASE WHEN "EDUCATN_C" IN ('E000', 'E001', 'E002', 'E003', 'E004', 'E005', 
                              'E006', 'E007', 'E008', 'E009', 'E010', 'E011')
        THEN 'LESS THAN HS'
        WHEN "EDUCATN_C" = 'E012'
        THEN 'HIGH SCHOOL'
        WHEN "EDUCATN_C" IS NULL OR "EDUCATN_C" = 'E099'
        THEN NULL
        ELSE 'ATL SOME COLLEGE'
        END AS "EDUCATN_C", 
    -- employment status
    CASE WHEN "EMPLYMNT_C" IS NULL OR "EMPLYMNT_C" LIKE ' %' 
        OR "EMPLYMNT_C" = 'UNK '
        THEN NULL
        ELSE "EMPLYMNT_C"
        END AS "EMPLYMNT_C",
    -- residential status
    CASE WHEN "RES_STAT_C" LIKE ' %' 
        THEN NULL ELSE "RES_STAT_C" END AS "RES_STAT_C",
    -- marital status
    CASE WHEN "MRT_STAT_C" = '09  ' OR "MRT_STAT_C" LIKE ' %' THEN NULL
        ELSE "MRT_STAT_C" END AS "MRT_STAT_C",
    -- disabilities
    CASE WHEN "DSBLTY_1_C" LIKE ' %' THEN 0
        WHEN "DSBLTY_1_C" IS NULL THEN NULL 
        ELSE 1 END AS "DSBLTY_1_F",
    -- household number
    CASE WHEN "HSEHLD_N" = 0 THEN NULL ELSE "HSEHLD_N" END AS "HSEHLD_N",
    -- household income ***
    "HSE_INC_A",
    -- proof of income
    "INC_PRF_C",
    -- date that income came from (to see how outdated/predated it is)
    "STRTCERT_D",
    -- program income came from (WIC is probably most accurate)
    "INC_PGM_ID_C",
    -- social support proxy (emergency contact present)
    "SOC_SUPPORT_F",
    -- medical risk indicator
    CASE WHEN "MED_RISK_F" = 'Y' THEN 1
        WHEN "MED_RISK_F" = 'N' THEN 0
        ELSE NULL END AS "MED_RISK_F",
    -- whether mother of infant was in WIC
    CASE WHEN "INF_WICM_F" = 'Y' THEN 1
        WHEN "INF_WICM_F" = 'N' THEN 0
        ELSE NULL END AS "INF_WICM_F",
    -- BBO/FCM enrollment during pregnancy
    "BBO_F", "FCM_F",
    -- delivery place
    CASE WHEN "DLV_PLC_C" LIKE ' %' 
        THEN NULL ELSE "DLV_PLC_C" END AS "DLV_PLC_C",
    -- delivery method
    CASE WHEN "DLV_MTHD_C" LIKE ' %'
        THEN NULL ELSE "DLV_MTHD_C" END AS "DLV_MTHD_C",
    -- pregnancy outcome
    "PREG_OTC_C",
    -- number of fetuses 
    CASE WHEN "FETUSES_N" = 0 THEN 1 ELSE "FETUSES_N" END AS "FETUSES_N",
    -- number of babies live at birth
    CASE WHEN "LIV_BRTH_N" = 0 THEN 1 ELSE "LIV_BRTH_N" END AS "LIV_BRTH_N",
    -- number of babies live at postpartum visit ***
    CASE WHEN "LIV_POST_N" = 0 THEN 1 ELSE "LIV_POST_N" END AS "LIV_POST_N",
    -- hospitalizations during pregnancy
    CASE WHEN "PREG_HSP_N" = 0 THEN 'NONE'
        WHEN "PREG_HSP_N" BETWEEN 1 AND 4 THEN 'LOW'
        WHEN "PREG_HSP_N" >= 5 THEN 'HIGH'
        ELSE NULL END AS "PREG_HSP_C",
    -- number of prenatal visits to a doctor ***
    "PMD_VSIT_N",
    -- smoking during 3 months prior to pregnancy
    CASE WHEN "PRE_SMK3_MTH_F" = 'Y' THEN 1
        WHEN "PRE_SMK3_MTH_F" = 'N' THEN 0
        ELSE NULL END AS "PRE_SMK3_MTH_F",
    -- smoking during 1st/2nd trimester of pregnancy
    "SMK_TRI1_F", "SMK_TRI2_F",
    -- smoking during 3rd trimester of pregnancy
    CASE WHEN "POST_SMK_TRI3_F" = 'Y' THEN 1
        WHEN "POST_SMK_TRI3_F" = 'N' THEN 0
        ELSE NULL END AS "POST_SMK_TRI3_F",
    -- drinking during 3 months prior to pregnancy
    CASE WHEN "PRE_DRK3_MTH_F" = 'Y' THEN 1
        WHEN "PRE_DRK3_MTH_F" = 'N' THEN 0
        ELSE NULL END AS "PRE_DRK3_MTH_F",
    -- drinking during 1st/2nd trimester of pregnancy
    "DRK_TRI1_F", "DRK_TRI2_F",
    -- drinking during 3rd trimester of pregnancy
    CASE WHEN "POST_DRK_TRI3_F" = 'Y' THEN 1
        WHEN "POST_DRK_TRI3_F" = 'N' THEN 0
        ELSE NULL END AS "POST_DRK_TRI3_F",
    -- quit-smoking interventions during 1st/2nd/3rd trimester of pregnancy
    "INV_TRI1_F", "INV_TRI2_F", "INV_TRI3_F",
    -- prenatal vitamin use during 1st/2nd/3rd trimester of pregnancy
    "VIT_TRI1_F", "VIT_TRI2_F", "VIT_TRI3_F",
    -- full address used for geocoding
    add_geo2,
    add_geo2 AS "FULL_ADD_GEO",
    -- confidence level for accuracy of geocoding
    confidence AS "CONF_GEO",
    -- fips county code 
    fips_county AS "FIPS_CTY_GEO",
    latitude,
    latitude AS "LAT_GEO",
    longitude,
    longitude AS "LONG_GEO",
    locality as "LOCALITY_GEO",
    region AS "REGION_GEO",
    -- county code name converted from fips code
    "CNTY_NAME_C" AS "CNTY_NAME_GEO",
    -- gestational length (of pregnancy)
    "PREG_WKS_N" AS "PREG_WKS_N_PRE",
    -- birthweight
    "WGT_GRM_N" AS "WGT_GRM_N_PRE",
    -- weeks after birth infant died (0 if did not die)
    "WKS_DIED_N" AS "WKS_DIED_N_PRE",
    -- number of stillborn fetuses
    "STILLBRN_N" AS "STILLBRN_N_PRE",
    -- infant disposition
    CASE WHEN "INF_DISP_C" LIKE ' %'
        THEN NULL ELSE "INF_DISP_C" END AS "INF_DISP_C_PRE",
    -- cause of infant death
    CASE WHEN "DTH_CAUS_C" LIKE ' %' 
        THEN 'NONE' ELSE "DTH_CAUS_C" END AS "DTH_CAUS_C_PRE",
    -- infant mortality outcome
    "INFM_OTC",
    -- NICU admission outcome
    "NICU_OTC",
    -- preterm birth outcome < 37 weeks
    "PTB_OTC",
    -- very preterm birth outcome < 32 weeks
    "VPTB_OTC",
    -- low birthweight outcome < 2500 g
    "LBW_OTC",
    -- very low birthweight outcome < 1500 g
    "VLBW_OTC",
    -- pregnancy complications outcome
    "PREG_COMPL_OTC",
    -- infant complications outcome
    "INF_COMPL_OTC",
    -- adverse birth outcome (any one of the adverse outcomes)
    "ADVB_OTC",
    -- very adverse birth outcome
    "VADVB_OTC"

    FROM core_birth_info2;
