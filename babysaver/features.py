import pandas as pd
import numpy as np
import re
from sklearn.preprocessing import PolynomialFeatures
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import Imputer
import itertools

def config_writer(dct, file_path):
    """
    This function allows you to specify a dictionary containing the 
    columns in the configuration file and then write the configuration file to 
    a csv. This makes mass config generation much faster.

    {
        'Features': ['INF_WICM_F'],
        'Include 707G?': 'Y',
        '707G Questions': [1, 2, 3, 4, 5],
        '707G Start Date': None,
        '707G End Date': 'yyyy-mm-dd',
        'Include 711?': 'Y',
        '711 Questions': [1, 2, 3, 4, 5],
        '711 Start Date': None,
        '711 End Date': None,
        'Include FCM?': 'Y',
        'Include BBO?': 'Y',
        'Include other?': 'Y',
        'Outcome': 'ADVB' 
    }
    """

    config = pd.DataFrame(dict([(k,pd.Series(v)) for k,v in dct.iteritems()]))
    col_names = ['Features', 'Include 707G?', '707G Questions', 
                 '707G Start Date', '707G End Date', 'Include 711?', 
                 '711 Questions', '711 Start Date', '711 End Date', 
                 'Include FCM?', 'Include BBO?', 'Include other?', 'Outcome']
    config = config[col_names] 
    config.to_csv(file_path, index=False)

def create_dummies(df, features, outcome, unique_id, missing_ind, create_ref=False):
    """ This function create_dummies returns a data frame """

    col_names = pd.Series(features)

    # only categorical variables with _C suffix should be dummied
    cat_vars = list(col_names[col_names.str.contains(r'(?i)_C$')])

    # only applicable to questions right now
    if missing_ind == 'missing_ind':
        q_vars = list(col_names[col_names.str.contains(r'(?i)_Q$')])
        # questions come as 1/0, so must turn into str first
        df_dum = pd.get_dummies(df[q_vars+[unique_id]].applymap(str).\
                                replace('nan', np.nan).set_index(unique_id),
                                dummy_na=True).reset_index(level=0)
        df = df.merge(df_dum, on=unique_id, how='left')
        # drop the 0 dummy column
        dummy0 = [x for x in df.columns.values if bool(re.search('_0.0$', x))]
        df = df.drop(q_vars+dummy0,  axis=1)

    # loop through and merge to preserve NA structure
    for co in cat_vars:
        temp = pd.get_dummies(df[[co, unique_id]].\
                set_index(unique_id).dropna()).reset_index(level=0)
        if create_ref:
            temp = temp.set_index(unique_id)
            index = temp.index # save index for later use 
            sums = temp.sum(axis=0) # get sums of each column
            temp = temp.append(sums, ignore_index=True)
            # sort the dummy columns by counts
            new_columns = temp.columns[temp.ix[temp.last_valid_index()].\
                                       argsort()]
            temp = temp[new_columns[::-1]]
            # drop column with largest count to use as ref category
            temp = temp.drop(temp.columns[0], axis=1).ix[0:(len(temp)-2),:]
            # recreate unique_id column for merging to base df
            temp[unique_id] = index
        df = df.merge(temp, on=unique_id, how='left')
    
    # drop original variables
    df = df.drop(cat_vars, axis=1)
    df.columns = [x.strip() for x in df.columns.values]
    # rearrange dataframe so outcome is rightmost
    new_col_names = pd.Series(df.columns.values)
    other_names = list(new_col_names[~new_col_names.str.\
                                     contains(r'(?i)_OTC$')])
    df = df[other_names+[outcome]]
    new_features = pd.Series(df.columns.values)
    new_features = list(new_features[~new_features.str.\
                                     contains(r'(?i)_OTC$|_D$|_I$')])

    return df, new_features


def standardize(df):
    """ This function standardize standardizes all continuous variables """
    
    # only non-missing
    my_data_complete = df.dropna()
    colnames = list(my_data_complete.columns.values)
    new_cols_list = [x for x in colnames # varnames of continuous vars
                     if (bool(re.search(r'(?i)_N$', x)))]
    idcols = [x for x in colnames # saving id varnames
              if not (bool(re.search(r'(?i)_N$', x)))]
    sub = my_data_complete[new_cols_list]
    subids = my_data_complete[idcols].reset_index(level=0)
    scaler = StandardScaler()
    standardized = pd.DataFrame(scaler.fit_transform(sub))
    # renaming standardized vars with original names
    standardized.columns = new_cols_list
    # combining standardized vars with the original data frame
    df_w_std = pd.DataFrame(subids.join(standardized))
    return df_w_std


def interactor(df):
    """ This function takes in a data frame and creates binary interaction
    terms from all numerical and categorical variables as well as the assessment
    questions, and outputs a data frame """
    

    my_data_complete = df.dropna()
    # interactions can only be done for non-missings
    colnames = list(my_data_complete.columns.values)
    # id and date columns
    id_cols_list = [x for x in colnames # only for continuous vars
                       if not (bool(re.search("_N$", x)) |
                        bool(re.search("_C$", x)) |
                          bool(re.search("_Q$", x)))]
    # actual feature columns - to make interactions from
    new_cols_list = [x for x in colnames # only for continuous vars
                       if (bool(re.search("_N$", x)) |
                        bool(re.search("_C$", x)) |
                          bool(re.search("_Q$", x)))]
    othervars = my_data_complete[id_cols_list]
    little_df = my_data_complete[new_cols_list]
    # computing all binary interaction terms
    poly = PolynomialFeatures(degree=2, interaction_only=True)
    theints = pd.DataFrame(poly.fit_transform(little_df))
    theints = theints.drop(theints.columns[0], axis=1) # dropping the first column
    theints.columns = list(new_cols_list + list(itertools.combinations(new_cols_list, 2)))
    # concatenating the interaction terms to the original data frame
    df = pd.DataFrame(othervars.join(theints))
    new_features = theints.columns.values
    return df, new_features 


def data_getter(config_file, conn, unique_identifier,
                dummies=True, std=True, interactions=False,
                impute=None, create_ref=False, holdout=None):
    """ 
    This function brings in the data from SQL, uses config file to determine
    user preferences and outputs a dictionary including the
    data frame, a list of features, the outcome variable, and
    the which assessment dates have been selected. There is the option to
    create dummies for assessment questions, standardize continuous variables,
    and create interaction terms for all variables.

    config_file: string, file path to config file
        **NOTE: in the config file, everything is case sensitive
    conn: connection to sql database 
    unique_identifier: string, what is the unique ID for your dataset?
    dummies: True/False, create dummies?
        default: True
    std: True/False, standardize continuous variables?
        default: True 
    interactions: True/False, generate all two-way interactions?
        default: False 
    impute: None, 'fill_mode', 'missing_ind', simple imputation strategies 
        default: None
        **NOTE: only imputes for question variables 
    create_ref: True/False, create reference column when generating dummies?
        default: False 
    """

    # load in config file
    if config_file.endswith('.xlsx'):
        config = pd.read_excel(config_file, sheetname='Sheet1')
    elif config_file.endswith('.csv'):
        config = pd.read_csv(config_file)
    else:
        raise ValueError('Config file is not of valid file type (Excel or csv)')

    # whether to include assessment questions
    include_707g = config['Include 707G?'][0]
    include_711 = config['Include 711?'][0]

    # append prefix/suffix to question numbers for sql query
    if include_707g == 'Y':
        questions_707g = ['707G_'+str(float(x))+'_Q' 
                          for x in config['707G Questions'].dropna()]
    else:
        questions_707g = []

    if include_711 == 'Y':
        questions_711 = ['711_'+str(float(x))+'_Q' 
                         for x in config['711 Questions'].dropna()]
    else:
        questions_711 = []

    questions_all = questions_707g + questions_711

    # get list of other features
    my_features = list(config['Features'].dropna()) + questions_all
    features = ['"' + x + '"' for x in my_features]
    all_features = features

    # determine time frame for assessments
    if include_707g == 'Y':
        start_707g = str(config['707G Start Date'][0])[0:10]
        if start_707g == 'nan':
            start_707g = '0001-01-01'
        end_707g = str(config['707G End Date'][0])[0:10]
        if end_707g == 'nan':
            end_707g = '9999-12-31'
        start_707g = "'" + start_707g + "'"
        end_707g = "'" + end_707g + "'"

    if include_711 == 'Y':
        start_711 = str(config['711 Start Date'][0])[0:10]
        if start_711 == 'nan':
            start_711 = '0001-01-01'
        end_711 = str(config['711 End Date'][0])[0:10]
        if end_711 == 'nan':
            end_711 = '9999-12-31'
        start_711 = "'" + start_711 + "'"
        end_711 = "'" + end_711 + "'"

    # determine which assessment dates to take
    dates = ''
    date_var = None
    if (include_707g == 'Y') & (include_711 == 'Y'):
        dates = '"707G_LT_D", "711_LT_D",'
        where_query = 'WHERE "707G_LT_D" BETWEEN ' + start_707g + ' AND ' + \
                      end_707g + ' AND "711_LT_D" BETWEEN ' + \
                      start_711 + ' AND ' + end_711
        date_var = '711_LT_D'
    elif (include_707g == 'Y') & (include_711 == 'N'):
        dates = '"707G_LT_D",'
        where_query = 'WHERE "707G_LT_D" BETWEEN ' + start_707g + ' AND ' + \
                      end_707g
        date_var = '707G_LT_D'
    elif (include_707g == 'N') & (include_711 == 'Y'):
        dates = '"711_LT_D",'
        where_query = 'WHERE "711_LT_D" BETWEEN ' + start_711 + ' AND ' + \
                      end_711
        date_var = '711_LT_D'

    # whether to include BBO/FCM people
    include_bbo = config['Include BBO?'][0]
    include_fcm = config['Include FCM?'][0]
    include_other = config['Include other?'][0]
    if include_bbo == 'N':
        where_query += ' AND "BBO_F" = 0'
    if include_fcm == 'N':
        where_query += ' AND "FCM_F" = 0'
    if (include_other == 'N') & (include_bbo == 'Y') & (include_fcm == 'Y'): 
        where_query += ' AND ("BBO_F" != 0 OR "FCM_F" != 0)'
    if (include_other == 'N') & (include_fcm == 'N'):
        where_query += ' AND "BBO_F" = 1'
    if (include_other == 'N') & (include_bbo == 'N'):
        where_query += ' AND "FCM_F" = 1'
    if (include_bbo == 'N') & (include_fcm == 'N'):
        where_query += ' AND ("BBO_F" = 0 AND "FCM_F" = 0)'
        
    # grab the outcome
    outcome = '"'+config['Outcome'][0]+'"'
    my_outcome = config['Outcome'][0]

    sql_id = '"'+unique_identifier+'"'

    # create sql query
    query = 'SELECT ' + sql_id + ', "LMP_D", "ACT_DLV_D",' + \
            dates + ','.join(all_features) + ',' + outcome + \
            ' FROM core_birth_info_rc ' + where_query

    df = pd.read_sql(query, conn)

    if impute == 'fill_mode':
        q_var = [x for x in my_features if bool(re.search(r'(?i)_Q$', x))]
        mode_imputer = Imputer(strategy='most_frequent')
        df.loc[:,q_var] = mode_imputer.fit_transform(df[q_var])

    if dummies:
        df, new_features  = create_dummies(df, my_features, my_outcome, 
                                           unique_identifier, 
                                           create_ref=create_ref, 
                                           missing_ind=impute)
    else: new_features = my_features 

    if std:
        colnames = list(df.columns.values)
        new_cols_list = [x for x in colnames # varnames of continuous vars
                         if (bool(re.search(r'(?i)_N$', x)))]
        if not new_cols_list: # only run standardizer for continuous variables
            print "data_getter: there are no continuous values to standardize"
        else: df = standardize(df)
    if interactions:
        df, new_features = interactor(df)

    df = df.dropna() 

    if holdout is not None:
        init_index = int((1-holdout)*len(df))
        holdout_df = df[init_index:]
        df = df[:init_index]
    else: 
        holdout_df = None

    dct = {
        'dataframe': df,
        'features': new_features,
        'outcome': my_outcome,
        'unique_id': unique_identifier,
        'config_file': config_file,
        'date': date_var,
        'holdout': holdout_df 
    }

    print 'data_getter: dataset has dimensions ' + str(df.shape)
    if holdout is not None: 
        print 'data_getter: holdout dataset has ' + str(holdout_df.shape)

    return dct

def general_data_getter(features, outcome, table, conn, n_rows=None, 
                        random=True, unique_identifier=None,
                        extra=None, dummies=True, std=True, interactions=False,
                        impute=None, create_ref=False, holdout=None):
    
    """ 
    This function brings in the data from SQL, uses config file to determine
    user preferences and outputs a dictionary including the
    data frame, a list of features, the outcome variable, and
    the which assessment dates have been selected. There is the option to
    create dummies for assessment questions, standardize continuous variables,
    and create interaction terms for all variables.

    features: string/list, which features to extract from sql table
    outcome: string, which outcome to extract from sql table
    table: string, which sql table to extract data from
    **NOTE: features, outcome, table are all case sensitive 
            i.e., if the column in postgres is uppercase, feature must be
            uppercase in list
    conn: connection to sql database 
    n_rows: None, int, how many rows to extract from sql table
        default: None (extract all rows)
    unique_identifier: None, string, what is the unique ID for your dataset?
        default: None (creates unique id)
    extra: string/list, any extra variables to extract that will not be used 
           as a feature or outcome (maybe useful for subsetting, sorting)
        default: None 
    dummies: True/False, create dummies?
        default: True
    std: True/False, standardize continuous variables?
        default: True 
    interactions: True/False, generate all two-way interactions?
        default: False 
    impute: None, 'fill_mode', 'missing_ind', simple imputation strategies 
        default: None
    create_ref: True/False, create reference column when generating dummies?
        default: False 
    """

    # make strings into list if arguments passed as string
    if type(extra) is not list: extra = [extra]
    if type(features) is not list: features = [features]

    # include extras in sql_features only if extras are specified
    if None not in extra:
        sql_features = ['"'+i+'"' for i in features+extra]
    else:
        sql_features = ['"'+i+'"' for i in features]

    # wrap in quotes because postgres is case sensitive
    sql_outcome = '"'+outcome+'"'

    # make sure n_rows is None or passed as a number
    if (type(n_rows) is not int) & (n_rows is not None):
       raise ValueError('n_rows must be an integer or None')

    # load in random sample or top X rows
    if n_rows is not None:
        if random:
            table += ' ORDER BY RANDOM() LIMIT ' + str(n_rows)
        else:
            table += ' LIMIT ' + str(n_rows)

    # if unique id is defined, grab it
    if unique_identifier is not None:
        sql_unique_id = '"'+unique_identifier+'"'
        query = 'SELECT ' + sql_unique_id + ','.join(sql_features) + \
                ', ' + sql_outcome + ' FROM ' + table
        df = pd.read_sql(query, conn)
    # if unique id is not defined, just make the unique id numbers
    elif unique_identifier is None: 
        query = 'SELECT ' + ','.join(sql_features) + \
                ', ' + sql_outcome + ' FROM ' + table 
        df = pd.read_sql(query, conn)
        unique_identifier = 'UNIQUE_ID_I'
        df[unique_identifier] = range(1, len(df)+1)
    
    # if impute is fill_mode, impute it
    if impute == 'fill_mode':
        mode_imputer = Imputer(strategy='most_frequent')
        df.loc[:,features] = mode_imputer.fit_transform(df[features])

    # create dummies
    if dummies:
        df, new_features  = create_dummies(df, features, outcome, 
                                           unique_identifier,
                                           create_ref=create_ref, 
                                           missing_ind=impute)
    else: new_features = features 

    if std:
        colnames = list(df.columns.values)
        new_cols_list = [x for x in colnames # varnames of continuous vars
                         if (bool(re.search(r'(?i)_N$', x)))]
        if not new_cols_list: # only run standardizer for continuous variables
            print "There are no continuous values to standardize"
        else: df = standardize(df)

    if interactions: df, new_features = interactor(df)

    df = df.dropna() 

    if holdout is not None:
        init_index = int((1-holdout)*len(df))
        holdout_df = df[init_index:]
        df = df[:init_index]
    else: 
        holdout_df = None

    dct = {
        'dataframe': df,
        'features': new_features,
        'outcome': outcome,
        'unique_id': unique_identifier,
        'extra': extra,
        'holdout': holdout_df
    }

    print 'data_getter: dataset has dimensions ' + str(df.shape)
    if holdout is not None: 
        print 'data_getter: holdout dataset has ' + str(holdout_df.shape)

    return dct
