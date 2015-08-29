/*
This script extracts 707G assessments from the different assessment tables
and creates two tables:

1) assess707g contains all assessment data from the original assessment tables
2) assess707g_births contains assessment data merged with birth outcomes, where
   the assessment date is within the timeframe of the pregnancy

Thus assess707g_births will contain only assessment questions and answers for 
the subset of assess707g that have birth outcome information recorded in our
birth cohort dataset

Written by: Ian Pan

Date created: 07/13/2015
Date updated: 07/14/2015
*/

-- CREATE TABLE assess707g AS 
-- 	SELECT * FROM assess2013 WHERE "ASSM_TYP_C" = '707G'
-- 		UNION 
-- 	SELECT * FROM assess2014 WHERE "ASSM_TYP_C" = '707G'
-- 		UNION
-- 	SELECT * FROM assess2015 WHERE "ASSM_TYP_C" = '707G';

-- UPDATE assess707g 
-- 	SET "QTS_RSLT_T" = replace("QTS_RSLT_T", ' ', '');

-- UPDATE assess707g 
-- 	SET "QTS_RSLT_T" = replace("QTS_RSLT_T", 'YES', 'Y');

-- UPDATE assess707g 
-- 	SET "QTS_RSLT_T" = replace("QTS_RSLT_T", 'NO', 'N');

-- UPDATE assess707g 
-- 	SET "QTS_RSLT_T" = replace("QTS_RSLT_T", 'U', 'Y');

CREATE TABLE assess707g_births1 AS
	WITH filter AS 
	(
		SELECT *, ROW_NUMBER() OVER (PARTITION BY "PART_ID_I",
			                                      "ACT_DLV_D",
			                                      "QUESTION_N"
			                         ORDER BY "ASSESS_D" DESC) AS rn
			FROM 
			(
				SELECT assess707g.*,
				core_birth_info2."UNI_PART_ID_I",
				core_birth_info2."LMP_D",
				core_birth_info2."ACT_DLV_D",
				core_birth_info2."PTB_OTC",
				core_birth_info2."VPTB_OTC",
				core_birth_info2."LBW_OTC",
				core_birth_info2."VLBW_OTC",
				core_birth_info2."NICU_OTC",
				core_birth_info2."ADVB_OTC"
					FROM assess707g
					INNER JOIN core_birth_info2 USING ("PART_ID_I")
					WHERE "ASSESS_D" BETWEEN "LMP_D" AND "ACT_DLV_D"
			) AS tmp
	)
	SELECT * FROM filter WHERE rn = 1;

CREATE TABLE assess_date_temp AS
	SELECT * 
		FROM 
		(
			SELECT *, 
				ROW_NUMBER() OVER (PARTITION BY "UNI_PART_ID_I" 
					               ORDER BY "ASSESS_D" DESC) 
					AS date_desc 
				FROM (
						SELECT *, 
							ROW_NUMBER() OVER (PARTITION BY "UNI_PART_ID_I" 
								               ORDER BY "ASSESS_D") 
								AS date_asc
							FROM 
							(
								SELECT DISTINCT "UNI_PART_ID_I", 
									"ASSESS_D" 
									FROM assess707g_births1
							) AS tmp
					) AS tmp2
		) AS tmp3 WHERE date_asc = 1 OR date_desc = 1;

CREATE TABLE assess_date_max AS
	SELECT * FROM 
	(
		SELECT *, 
			ROW_NUMBER() OVER (PARTITION BY "UNI_PART_ID_I"
			                   ORDER BY count DESC) 
				AS date_mode
			FROM
			(
				SELECT "UNI_PART_ID_I", "ASSESS_D", COUNT("ASSESS_D") 
					FROM assess707g_births1 
					GROUP BY 1,2 
					ORDER BY 3 DESC
			) AS tmp
	) AS tmp2 WHERE date_mode = 1;

CREATE TABLE assess707g_births2 AS
	WITH filter_asc AS 
	(
		SELECT * FROM assess_date_temp WHERE date_asc = 1
	)
	SELECT assess707g_births1.*, 
		filter_asc."ASSESS_D" AS "FIRST_ASSESS_D" 
		FROM assess707g_births1 
		LEFT JOIN filter_asc USING ("UNI_PART_ID_I");

DROP TABLE assess707g_births1;

CREATE TABLE assess707g_births AS
	WITH filter_desc AS (
		SELECT * FROM assess_date_temp WHERE date_desc = 1
	)
	SELECT assess707g_births2.*, 
		filter_desc."ASSESS_D" AS "LAST_ASSESS_D",
		assess_date_max."ASSESS_D" AS "MOST_ASSESS_D" 
		FROM assess707g_births2
		LEFT JOIN filter_desc USING ("UNI_PART_ID_I")
		LEFT JOIN assess_date_max USING ("UNI_PART_ID_I");

DROP TABLE assess707g_births2, assess_date_temp, assess_date_max;