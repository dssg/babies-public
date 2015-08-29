/*
This script merges ACS tract-level estimates to the individual-level dataset.
Written by: Rashida Brown

*/

CREATE TABLE core_birth_info_rc_tct AS
    SELECT core_birth_info_rc.*, 
        acs_tract.tract_data_fin2.tct_perc_less_hs_ed_n, 
        acs_tract.tract_data_fin2.tct_perc_unemployed_n,
        acs_tract.tract_data_fin2.tct_perc_m_niwf_n,
        acs_tract.tract_data_fin2.tct_perc_hsld_crwd_n,
        acs_tract.tract_data_fin2.tct_perc_hsld_rent_n,
        acs_tract.tract_data_fin2.tct_perc_hsld_vac_n,
        acs_tract.tract_data_fin2.tct_perc_rent_cost_n,
        acs_tract.tract_data_fin2.tct_med_hsld_val_n,
        acs_tract.tract_data_fin2.tct_perc_own_cost_n,
        acs_tract.tract_data_fin2.tct_perc_m_mngmnt_n,
        acs_tract.tract_data_fin2.tct_perc_m_prof_n,
        acs_tract.tract_data_fin2.tct_perc_f_mngmnt_n,
        acs_tract.tract_data_fin2.tct_perc_f_prof_n,
        acs_tract.tract_data_fin2.tct_perc_blw_fpl_n,
        acs_tract.tract_data_fin2.tct_perc_novehic_n,
        acs_tract.tract_data_fin2.tct_perc_pubassist_n,
        acs_tract.tract_data_fin2.tct_perc_inc_lt_30k_n,
        acs_tract.tract_data_fin2.tct_perc_f_hshld_n,
        acs_tract.tract_data_fin2.tct_perc_blk_n,
        acs_tract.tract_data_fin2.tct_perc_hisp_n,
        acs_tract.tract_data_fin2.tct_perc_spanish_lang_n,
        acs_tract.tract_data_fin2.tct_perc_foreign_born_n,
        acs_tract.tract_data_fin2.tct_perc_longt_res_n,
        acs_tract.tract_data_fin2.tct_perc_65older_n,
        wic_agg2.tct_total_wic_clin_n, 
        bbo_agg2.tct_total_bbo_clin_n, 
        fcm_agg2.tct_total_fcm_clin_n
        FROM core_birth_info_rc
        LEFT JOIN acs_tract.tract_data_fin2 USING ("FIPS_TRACT_GEO")
        LEFT JOIN wic_agg2 USING ("FIPS_TRACT_GEO")
        LEFT JOIN bbo_agg2 USING ("FIPS_TRACT_GEO")
        LEFT JOIN fcm_agg2 USING ("FIPS_TRACT_GEO");
