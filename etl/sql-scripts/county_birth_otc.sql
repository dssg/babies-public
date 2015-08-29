/*
This script creates tract and county aggregates of Cornerstone birth outcomes
Written by: Rashida Brown

*/



CREATE TABLE geo_agg_county_yr AS
    SELECT "FIPS_CTY_GEO", "ACT_DLV_YR",
        COUNT("UNI_PART_ID_I") AS total_births_n,
        SUM("PTB_OTC") AS total_ptb_n,
        AVG("PTB_OTC") AS rate_ptb_n,
        SUM("VPTB_OTC") AS total_vptb_n,
        AVG("VPTB_OTC") AS rate_vptb_n,
        SUM("LBW_OTC") AS total_lbw_n,
        AVG("LBW_OTC") AS rate_lbw_n,
        SUM("VLBW_OTC") AS total_vlbw_n,
        AVG("VLBW_OTC") AS rate_vlbw_n,
        SUM("INFM_OTC") AS total_infm_n,
        AVG("INFM_OTC") AS rate_infm_n,
        SUM("NICU_OTC") AS total_nicu_n,
        AVG("NICU_OTC") AS rate_nicu_n,
        SUM("PREG_COMPL_OTC") AS total_pregCompl_n,
        AVG("PREG_COMPL_OTC") AS rate_pregCompl_n,
        SUM("INF_COMPL_OTC") AS total_infCompl_n,
        AVG("INF_COMPL_OTC") AS rate_infCompl_n,
        SUM("ADVB_OTC") AS total_advb_n,
        AVG("ADVB_OTC") AS rate_advb_n,
        AVG("WGT_GAIN_N") AS avg_wgtGain_n,
        AVG("PRE_BMI_N") AS avg_preBMI_n,
        AVG("POST_SMK_TRI3_F") AS rate_smkTri3_n,
        AVG("POST_DRK_TRI3_F") AS rate_drkTri3_n
        FROM core_birth_info_rc WHERE "REGION_GEO"='IL'
        GROUP BY 1,2;

CREATE TABLE geo_agg_county_allyr AS
    SELECT "FIPS_CTY_GEO", 
        COUNT("UNI_PART_ID_I") AS total_births_n,
        SUM("PTB_OTC") AS total_ptb_n,
        AVG("PTB_OTC") AS rate_ptb_n,
        SUM("VPTB_OTC") AS total_vptb_n,
        AVG("VPTB_OTC") AS rate_vptb_n,
        SUM("LBW_OTC") AS total_lbw_n,
        AVG("LBW_OTC") AS rate_lbw_n,
        SUM("VLBW_OTC") AS total_vlbw_n,
        AVG("VLBW_OTC") AS rate_vlbw_n,
        SUM("INFM_OTC") AS total_infm_n,
        AVG("INFM_OTC") AS rate_infm_n,
        SUM("NICU_OTC") AS total_nicu_n,
        AVG("NICU_OTC") AS rate_nicu_n,
        SUM("PREG_COMPL_OTC") AS total_pregCompl_n,
        AVG("PREG_COMPL_OTC") AS rate_pregCompl_n,
        SUM("INF_COMPL_OTC") AS total_infCompl_n,
        AVG("INF_COMPL_OTC") AS rate_infCompl_n,
        SUM("ADVB_OTC") AS total_advb_n,
        AVG("ADVB_OTC") AS rate_advb_n,
        AVG("WGT_GAIN_N") AS avg_wgtGain_n,
        AVG("PRE_BMI_N") AS avg_preBMI_n,
        AVG("POST_SMK_TRI3_F") AS rate_smkTri3_n,
        AVG("POST_DRK_TRI3_F") AS rate_drkTri3_n
        FROM core_birth_info_rc WHERE "REGION_GEO"='IL'
        GROUP BY 1;

CREATE TABLE geo_agg_tract_yr2 AS
    SELECT "FIPS_TRACT_GEO", "ACT_DLV_YR","CNTY_NAME_GEO",
        COUNT("UNI_PART_ID_I") AS tct_total_births_n,
        SUM("PTB_OTC") AS tct_total_ptb_n,
        AVG("PTB_OTC") AS tct_rate_ptb_n,
        SUM("VPTB_OTC") AS tct_total_vptb_n,
        AVG("VPTB_OTC") AS tct_rate_vptb_n,
        SUM("LBW_OTC") AS tct_total_lbw_n,
        AVG("LBW_OTC") AS tct_rate_lbw_n,
        SUM("VLBW_OTC") AS tct_total_vlbw_n,
        AVG("VLBW_OTC") AS tct_rate_vlbw_n,
        SUM("INFM_OTC") AS tct_total_infm_n,
        AVG("INFM_OTC") AS tct_rate_infm_n,
        SUM("NICU_OTC") AS tct_total_nicu_n,
        AVG("NICU_OTC") AS tct_rate_nicu_n,
        SUM("PREG_COMPL_OTC") AS tct_total_pregCompl_n,
        AVG("PREG_COMPL_OTC") AS tct_rate_pregCompl_n,
        SUM("INF_COMPL_OTC") AS tct_total_infCompl_n,
        AVG("INF_COMPL_OTC") AS tct_rate_infCompl_n,
        SUM("ADVB_OTC") AS tct_total_advb_n,
        AVG("ADVB_OTC") AS tct_rate_advb_n,
        SUM("ADVB1_OTC") AS tct_total_advb1_n,
        AVG("ADVB1_OTC") AS tct_rate_advb1_n,
        AVG("WGT_GAIN_N") AS tct_avg_wgtGain_n,
        AVG("PRE_BMI_N") AS tct_avg_preBMI_n,
        AVG("POST_SMK_TRI3_F") AS tct_rate_smkTri3_n,
        AVG("POST_DRK_TRI3_F") AS tct_rate_drkTri3_n,
        SUM("INF_WICM_F") AS tct_total_wic_n,
        SUM("FCM_F") AS tct_total_fcm_n,
        SUM("BBO_F") AS tct_total_bbo_n
        FROM core_birth_info_rc WHERE "REGION_GEO"='IL' AND "ACT_DLV_YR">=2009 AND "ACT_DLV_YR"<=2014
        GROUP BY 1,2,3;


CREATE TABLE geo_agg_tract_allyr2 AS
    SELECT "FIPS_TRACT_GEO", "CNTY_NAME_GEO",
        COUNT("UNI_PART_ID_I") AS tct_total_births_n,
        SUM("PTB_OTC") AS tct_total_ptb_n,
        AVG("PTB_OTC") AS tct_rate_ptb_n,
        SUM("VPTB_OTC") AS tct_total_vptb_n,
        AVG("VPTB_OTC") AS tct_rate_vptb_n,
        SUM("LBW_OTC") AS tct_total_lbw_n,
        AVG("LBW_OTC") AS tct_rate_lbw_n,
        SUM("VLBW_OTC") AS tct_total_vlbw_n,
        AVG("VLBW_OTC") AS tct_rate_vlbw_n,
        SUM("INFM_OTC") AS tct_total_infm_n,
        AVG("INFM_OTC") AS tct_rate_infm_n,
        SUM("NICU_OTC") AS tct_total_nicu_n,
        AVG("NICU_OTC") AS tct_rate_nicu_n,
        SUM("PREG_COMPL_OTC") AS tct_total_pregCompl_n,
        AVG("PREG_COMPL_OTC") AS tct_rate_pregCompl_n,
        SUM("INF_COMPL_OTC") AS tct_total_infCompl_n,
        AVG("INF_COMPL_OTC") AS tct_rate_infCompl_n,
        SUM("ADVB_OTC") AS tct_total_advb_n,
        AVG("ADVB_OTC") AS tct_rate_advb_n,
        SUM("ADVB1_OTC") AS tct_total_advb1_n,
        AVG("ADVB1_OTC") AS tct_rate_advb1_n,
        AVG("WGT_GAIN_N") AS tct_avg_wgtGain_n,
        AVG("PRE_BMI_N") AS tct_avg_preBMI_n,
        AVG("POST_SMK_TRI3_F") AS tct_rate_smkTri3_n,
        AVG("POST_DRK_TRI3_F") AS tct_rate_drkTri3_n,
        SUM("INF_WICM_F") AS tct_total_wic_n,
        SUM("FCM_F") AS tct_total_fcm_n,
        SUM("BBO_F") AS tct_total_bbo_n
        FROM core_birth_info_rc WHERE "REGION_GEO"='IL' AND "ACT_DLV_YR">=2009 AND "ACT_DLV_YR"<=2014
        GROUP BY 1,2;
