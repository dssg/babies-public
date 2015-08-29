# Getting the data

Data were extracted as individual tables in either `.mdb` or `.dbf` file formats from IDHS' Cornerstone database and put in Box. Individual files were downloaded locally and then uploaded to the Amazon Web Services instance. 

The 2013 Natality data from the CDC Vital Statistics was downloaded in `.csv` format via:
http://www.nber.org/data/natality.html

# Converting to CSV 

In order to upload the data to our PostgreSQL database, we first converted them to `.csv` on our AWS instance using `mdb-tools`.  

    mdb-tables Prenatal.mdb
    >> Prental records since 1-1-2010 Prental_records_since_1_1_2010_ExportErrors
    mdb-export Prenatal.mdb "Prental records since 1-1-2010" > prenatal.csv

To convert `.dbf` files, we opened the files locally in Microsoft Excel and saved them as `.csv` files. Any other similar program (OpenOffice Calc, LibreOffice Calc) will also work.

In `PartEnroll.csv` (or any future file that contains the data from the Participant Enrollment screen in Cornerstone), there are names and Social Security numbers. We used this command to remove them.

    csvcut -C 4-14,17,18 PartEnroll.csv > PartEnroll_noname.csv
    
Depending on how future data is extracted from this table, the column numbers might be different. 

# Uploading to PostgreSQL

With our data in `.csv` format, we can use `psql_uploader.py` to upload the data to Postgres. 

To upload a dataset to a new table (or to append to an existing table):

    python psql_uploader.py -i filepath -t tablename

To overwrite an existing table:

    python psql_uploader.py -o -i filepath -t tablename

To use a schema other than `public`:

    python psql_uploader.py -i filepath -t tablename -s schemaname

Based on the suffixes of the variables in the Cornerstone database, the script automatically infers types. Otherwise, the default type is `text`.

# Running ETL

To run the ETL pipeline after all the data has been uploaded to Postgres, call the `main()` function in `etl_mydata.py`. Note that some parts of the ETL take a long time; specifically, geocoding (running `geocoder()`) of 500,000 addresses takes about 1 hour and converting 500,000 latlongs into census tracts (running `coord2tract()`) takes 12-18 hours. If you are only working with individual-level assessment data and don't need the geographic data then you can comment those parts out to save time.  

The pipeline runs the SQL scripts in `sql-scripts` and assumes you have the following tables:

    prenatal
    postpart
    partenroll
    program
    assess711
    assess2013
    assess2014
    assess2015
    address2
    birth
    catghist
    hlthvsit

In the future, it makes more sense to have `assess711` and `assess707g` rather than assessments by year.

# SQL Scripts

In `sql-scripts`, we have various SQL scripts that are called in `main()` of `etl_mydata.py` in the following order:

## Individual level

* merge_data.sql - merges data from different tables (`prenatal`, `postpart`, etc.)
* indiv_address.sql - prepares addresses for geocoding
* merge_indiv_geocode.sql - after geocoding, merge the geocode information with all the other variables
* recode_data.sql - recode the data to get rid of implausible values and prepare the data for analysis
* build_711_dataset.sql - prepare 711 assessment dataset for pivoting
* build_707g_dataset.sql - prepare 707G assessment dataset for pivoting
* merge_assess_qts.sql - merge assessment questions with all the other variables
* indiv_census_tracts.sql - after converting latlongs to tracts, merge tracts with all the other variables

## Geographic level

* clinic_address.sql - prepare clinic addresses for geocoding
* clinic_census_tracts.sql - after converting latlongs to tracts, merge tracts to clinic tables
* census_cleaning.sql - assuming you have the ACS dump in postgres as a separate schema `acs`, cleans the data and extracts relevant variables
* county_birth_otc.sql - create table with aggregate birth outcomes at county and census tract levels
* merge_tractvars.sql - merge census tract variables to individual level dataset

## Vital Statistics

We extracted data from the Vital Statistics to use in our pipeline. `vitals.sql` contains the code used to generate the pipeline-ready table from the original file.

# Important Notes

We didn't `CREATE INDEX` or keys on any variables, which should be done for best practice. 
