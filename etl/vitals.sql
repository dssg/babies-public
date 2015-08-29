"""
This script creates tables from the vital statistics data for use
in exploratory analyses. See python script vitals.py for these analyses.

Written by Laura Nolan

Created on: 7/27/2015
Updated on: 8/16/2015

"""

-- Creating table for just select variables for vital stats analyses

DROP TABLE vitals;

CREATE TABLE vitals AS
SELECT rf_diab AS rf_diab_c, rf_gest AS rf_gest_c, rf_phyp AS rf_phyp_c,
rf_ghyp AS rf_ghyp_c, rf_eclam AS rf_eclam_c, rf_ppoutc AS rf_ppoutc_c,
rf_ppterm AS rf_ppterm_c, pwgt_r AS pwgt_r_n, ip_gono AS ip_gono_c, ip_syph AS ip_syph_c,
ip_chlam AS ip_chlam_c, ip_hepatb AS ip_hepatb_c, ip_hepatc AS ip_hepatc_c, mager AS mager_n,
meduc AS meduc_n, dbwt AS dbwt_otc, dplural AS dplural_c,
ab_nicu AS ab_nicu_otc, pay_rec AS pay_rec_c,
cig_0 AS cig_0_n, cig_1 AS cig_1_n, cig_2 AS cig_2_n, cig_3 AS cig_3_n,
combgest AS combgest_otc,
ocntypop AS ocntypop_c, bfacil AS bfacil_c,
rectype AS rectype_c, restatus AS restatus_c,
umhisp AS umhisp_c, mracehisp AS mracehisp_c, mar_p AS mar_p_c,
mar AS mar_c, fagecomb AS fagecomb_n, fracerec AS fracerec_c,
ufhisp AS ufhisp_c, fracehisp AS fracehisp_c, feduc AS feduc_c,
priorterm AS priorterm_c,
ilpcv_dob AS ilpcv_dob_n, precare AS precare_c,
uprevis AS uprevis_n, wic AS wic_c
FROM natl2013;

ALTER TABLE vitals ALTER COLUMN combgest_otc TYPE INTEGER USING(combgest_otc::INTEGER);
ALTER TABLE vitals ALTER COLUMN dbwt_otc TYPE INTEGER USING(dbwt_otc::INTEGER);

DROP TABLE vitals_rec;

CREATE TABLE vitals_rec AS
SELECT *,
CASE WHEN combgest_otc=99 THEN NULL
	WHEN combgest_otc<37 THEN 1
	WHEN combgest_otc>=37 THEN 0
END AS ptb_otc,
CASE WHEN dbwt_otc=9999 THEN NULL
	WHEN dbwt_otc<2500 THEN 1
	WHEN dbwt_otc>=2500 THEN 0
END AS lbw_otc,
CASE WHEN dbwt_otc=9999 OR combgest_otc<37 THEN NULL
	WHEN dbwt_otc<2500 AND combgest_otc>=37 THEN 1
	WHEN dbwt_otc>=2500 AND combgest_otc>=37 THEN 0
END AS tlbw_otc
FROM vitals;

DROP TABLE vitals_rec2;

CREATE TABLE vitals_rec2 AS
SELECT *,
CASE WHEN ptb_otc IS NULL AND lbw_otc IS NULL AND ab_nicu_otc='U' THEN NULL
	WHEN ptb_otc='1' OR lbw_otc='1' OR ab_nicu_otc='Y' THEN 1
    WHEN ptb_otc='0' AND lbw_otc='0' AND ab_nicu_otc='N' THEN 0
END AS advb_otc,
CASE WHEN rf_diab_c='U' AND rf_gest_c='U' AND rf_phyp_c='U' AND rf_ghyp_c='U' AND rf_eclam_c='U' THEN NULL
	WHEN rf_diab_c='Y' OR rf_gest_c='Y' OR rf_phyp_c='Y' OR rf_ghyp_c='Y' OR rf_eclam_c='Y' THEN 1
    WHEN  rf_diab_c='N' AND rf_gest_c='N' AND rf_phyp_c='N' AND rf_ghyp_c='N' AND rf_eclam_c='N' THEN 0
    END AS pregcomp_c
FROM vitals_rec;

ALTER TABLE vitals_rec2 ALTER COLUMN pwgt_r_n TYPE INTEGER USING(pwgt_r_n::INTEGER);
ALTER TABLE vitals_rec2 ALTER COLUMN meduc_n TYPE INTEGER USING(meduc_n::INTEGER);
ALTER TABLE vitals_rec2 ALTER COLUMN cig_0_n TYPE INTEGER USING(cig_0_n::INTEGER);
ALTER TABLE vitals_rec2 ALTER COLUMN cig_1_n TYPE INTEGER USING(cig_1_n::INTEGER);
ALTER TABLE vitals_rec2 ALTER COLUMN cig_2_n TYPE INTEGER USING(cig_2_n::INTEGER);
ALTER TABLE vitals_rec2 ALTER COLUMN cig_3_n TYPE INTEGER USING(cig_3_n::INTEGER);
ALTER TABLE vitals_rec2 ALTER COLUMN mager_n TYPE INTEGER USING(mager_n::INTEGER);

DROP TABLE vitals_rec3;

CREATE TABLE vitals_rec3 AS
SELECT *,
CASE WHEN pwgt_r_n=999 THEN NULL
	ELSE pwgt_r_n
	END AS pwgt_r_rec_n,
CASE WHEN ip_gono_c='U' THEN NULL
	ELSE ip_gono_c
	END AS ip_gono_rec_c,
CASE WHEN ip_syph_c='U' THEN NULL
	ELSE ip_syph_c
	END AS ip_syph_rec_c,
CASE WHEN ip_chlam_c='U' THEN NULL
	ELSE ip_chlam_c
	END AS ip_chlam_rec_c,
CASE WHEN ip_hepatb_c='U' THEN NULL
	ELSE ip_hepatb_c
	END AS ip_hepatb_rec_c,
CASE WHEN ip_hepatc_c='U' THEN NULL
	ELSE ip_hepatc_c
	END AS ip_hepatc_rec_c,
CASE WHEN meduc_n IS NULL OR meduc_n=9 THEN NULL
	ELSE meduc_n
	END AS meduc_rec_n,
CASE WHEN ab_nicu_otc IS NULL OR ab_nicu_otc='U' THEN NULL
	WHEN ab_nicu_otc='Y' THEN 1
	WHEN ab_nicu_otc='N' THEN 0
	END AS ab_nicu_rec_otc,
CASE WHEN cig_0_n=99 OR cig_0_n IS NULL THEN NULL
    ELSE cig_0_n
    END AS cig_0_rec_n,
CASE WHEN cig_1_n=99 OR cig_1_n IS NULL THEN NULL
    ELSE cig_1_n
    END AS cig_1_rec_n,
CASE WHEN cig_2_n=99 OR cig_2_n IS NULL THEN NULL
    ELSE cig_2_n
    END AS cig_2_rec_n,
CASE WHEN cig_3_n=99 OR cig_3_n IS NULL THEN NULL
    ELSE cig_3_n
    END AS cig_3_rec_n,
CASE WHEN pay_rec_c = '9' OR pay_rec_c IS NULL THEN NULL
    ELSE pay_rec_c
    END AS pay_rec_rec_c,
CASE WHEN "ip_gono_c"='Y' OR "ip_syph_c"='Y' OR "ip_chlam_c"='Y' OR "ip_hepatb_c"='Y' OR "ip_hepatc_c"='Y' THEN 1
    WHEN "ip_gono_c"='N' OR "ip_syph_c"='N' OR "ip_chlam_c"='N' OR "ip_hepatb_c"='N' OR "ip_hepatc_c"='N' THEN 0
    WHEN "ip_gono_c"='U' OR "ip_syph_c"='U' OR "ip_chlam_c"='U' OR "ip_hepatb_c"='U' OR "ip_hepatc_c"='U' THEN 0
    END AS sexdis_rec_f
FROM vitals_rec2;

DROP TABLE vitals_all;

CREATE TABLE vitals_all AS
SELECT *,
CASE WHEN bfacil_c='9' OR bfacil_c='7' OR bfacil_c IS NULL THEN NULL
	ELSE bfacil_c
	END AS bfacil_rec_c,
CASE WHEN umhisp_c='5' OR umhisp_c='9' THEN NULL
	ELSE umhisp_c
	END AS umhisp_rec_c,
CASE WHEN mracehisp_c='9' OR mracehisp_c=NULL THEN NULL
	ELSE mracehisp_c
	END AS mracehisp_rec_c,
CASE WHEN mar_p_c='U' OR mar_p_c='X' OR mar_p_c IS NULL THEN NULL
	ELSE mar_p_c
	END AS mar_p_rec_c,
CASE WHEN mar_c='U' OR mar_c IS NULL THEN NULL
	ELSE mar_c
	END AS mar_rec_c,
CASE WHEN fracerec_c='9' OR fracerec_c is NULL THEN NULL
	ELSE fracerec_c
	END AS fracerec_rec_c,
CASE WHEN ufhisp_c='9' OR ufhisp_c IS NULL THEN NULL
	ELSE ufhisp_c
	END AS ufhisp_rec_c,
CASE WHEN fracehisp_c='9' OR fracehisp_c IS NULL THEN NULL
	ELSE fracehisp_c
	END AS fracehisp_rec_c,
CASE WHEN feduc_c='9' OR feduc_c IS NULL THEN NULL
	ELSE feduc_c
	END AS feduc_rec_c,
CASE WHEN priorterm_c='99' OR priorterm_c IS NULL THEN NULL
	ELSE priorterm_c
	END AS priorterm_rec_c,
CASE WHEN precare_c='99' OR precare_c IS NULL THEN NULL
	ELSE precare_c
	END AS precare_rec_c,
CASE WHEN wic_c='U' OR wic_c IS NULL THEN NULL
	ELSE wic_c
	END AS wic_rec_c,
CASE WHEN rf_ppoutc_c='U' OR rf_ppoutc_c IS NULL THEN NULL
	ELSE rf_ppoutc_c
	END AS rf_ppoutc_rec_c,
CASE WHEN ilpcv_dob_n='99' OR ilpcv_dob_n IS NULL THEN NULL
	ELSE ilpcv_dob_n
	END AS ilpcv_dob_rec_n,
CASE WHEN uprevis_n='99' OR uprevis_n IS NULL THEN NULL
	ELSE uprevis_n
	END AS uprevis_rec_n,
CASE WHEN fagecomb_n='99'OR fagecomb_n IS NULL THEN NULL
	ELSE fagecomb_n
	END AS fagecomb_rec_n,
CASE WHEN rf_diab_c='U' THEN NULL
	ELSE rf_diab_c
	END AS rf_diab_rec_c,
CASE WHEN rf_gest_c='U' THEN NULL
	ELSE rf_gest_c
	END AS rf_gest_rec_c,
CASE WHEN rf_phyp_c='U' THEN NULL
	ELSE rf_phyp_c
	END AS rf_phyp_rec_c,
CASE WHEN rf_ghyp_c='U' THEN NULL
	ELSE rf_ghyp_c
	END AS rf_ghyp_rec_c,
CASE WHEN rf_eclam_c='U' THEN NULL
	ELSE rf_eclam_c
	END AS rf_eclam_rec_c,
CASE WHEN rf_ppterm_c='U' THEN NULL
	ELSE rf_ppterm_c
	END AS rf_ppterm_rec_c
FROM vitals_rec3;

drop table forheatmap;

-- Recoding categorical to continuous
ALTER TABLE vitals_all ALTER COLUMN ilpcv_dob_rec_n TYPE INTEGER USING(ilpcv_dob_rec_n::INTEGER);
ALTER TABLE vitals_all ALTER COLUMN uprevis_rec_n TYPE INTEGER USING(uprevis_rec_n::INTEGER);
ALTER TABLE vitals_all ALTER COLUMN fagecomb_rec_n TYPE INTEGER USING(fagecomb_rec_n::INTEGER);

-- Creating a table of only 0's and 1's for vital statistics heat map

CREATE TABLE forheatmap AS
SELECT rf_diab_rec_c, rf_gest_rec_c, rf_phyp_rec_c, rf_ghyp_rec_c,
            rf_eclam_rec_c, rf_ppoutc_rec_c, rf_ppterm_rec_c, dplural_c,
            sexdis_rec_f, meduc_rec_n, cig_0_rec_n, cig_1_rec_n,
            cig_2_rec_n, cig_3_rec_n, pay_rec_rec_c, advb_otc, pwgt_r_rec_n,
CASE WHEN rf_diab_c='Y' THEN 1
	WHEN rf_diab_c='N' THEN 0
	ELSE NULL
	END AS rf_diab_c_rec,
CASE WHEN rf_gest_c='Y' THEN 1
	WHEN rf_gest_c='N' THEN 0
	ELSE NULL
	END AS rf_gest_c_rec,
CASE WHEN rf_phyp_c='Y' THEN 1
	WHEN rf_phyp_c='N' THEN 0
	ELSE NULL
	END AS rf_phyp_c_rec,
CASE WHEN rf_ghyp_c='Y' THEN 1
	WHEN rf_ghyp_c='N' THEN 0
	ELSE NULL
	END AS rf_ghyp_c_rec,
CASE WHEN rf_eclam_c='Y' THEN 1
	WHEN rf_eclam_c='N' THEN 0
	ELSE NULL
	END AS rf_eclam_c_rec,
CASE WHEN rf_ppoutc_c='Y' THEN 1
	WHEN rf_ppoutc_c='N' THEN 0
	ELSE NULL
	END AS rf_ppoutc_c_rec,
CASE WHEN rf_ppterm_c='Y' THEN 1
	WHEN rf_ppterm_c='N' THEN 0
	ELSE NULL
	END AS rf_ppterm_c_rec
FROM vitals_all;

ALTER TABLE forheatmap ALTER COLUMN sexdis_rec_f TYPE INTEGER USING(sexdis_rec_f::INTEGER);

-- Below I create a table to be used for counting the number of women taking the
-- first 707G version, those determined to be eligible for BBO,
-- and those who actually enroll in the program

CREATE TABLE eligibility AS
SELECT * FROM core_birth_info_rc WHERE
"707G_LT_D" IS NOT NULL AND "707G_LT_D" < '2014-07-01' AND "ADVB_OTC" IS NOT NULL;
