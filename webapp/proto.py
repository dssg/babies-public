import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import json
import sys
from sklearn.externals import joblib
import os

def run_all():

    # connect to postgres 
    params = json.load(open('/home/ipan/passwords/psql_psycopg2.password', 'r'))

    try:
        conn = psycopg2.connect(**params)
        conn.autocommit
        cur = conn.cursor()

    except:
        print('Unable to connect to database')

    #  import from babysaver
    sys.path.insert(0, '/home/ipan/babies/')
    from babysaver import features
    from babysaver import models
    from babysaver.models import WeightedQuestions
    from sklearn.linear_model import LogisticRegression
    from babysaver import evaluation

    # specify dat configuration in a dictionary
    config_add1 = {'Features': None, 
                               'Include 707G?': 'Y', 
                               '707G Questions': range(35,52), 
                               '707G Start Date': '2014-07-01', 
                               '707G End Date': None,
                               'Include 711?': 'N', 
                               '711 Questions': None, 
                               '711 Start Date': None, 
                               '711 End Date': None, 
                               'Include FCM?': 'Y', 
                               'Include BBO?': 'Y', 
                               'Include other?': 'Y', 
                               'Outcome': 'ADVB1_OTC'}

    # use config_writer to write dictionary to csv file 
    features.config_writer(config_add1, '/home/ipan/configs/config_add1.csv')
    # then use that csv file to load in the data 
    data_dct = features.data_getter('/home/ipan/configs/config_add1.csv', 
                                    conn=conn, 
                                    unique_identifier='UNI_PART_ID_I',  
                                    impute='fill_mode',
                                    interactions=False)

    # specify hyperparameter lists
    c_list = [1e-4, 1e-3, 0.01, 0.1, 1, 10, 100, 1e3, 1e4, 1e20]
    penalties = ['l2']
    class_wgts = [None, 'auto']

    wgt_schemes = ['odds_ratio_relative', 'odds_ratio_absolute', 
                   'marginal_effects', 'positive_coefs']

    # specify classifier dictionaries 
    expand_wgt = {'clf': WeightedQuestions,
                  'param_dict': {'C': c_list,
                                 'penalty': penalties, 
                                 'class_weight': class_wgts,
                                 'weight_scheme': wgt_schemes,
                                 'round_dec': [1]
                                }
                 }

    simple_wgt = {'clf': WeightedQuestions,
                  'param_dict': {'C': c_list,
                                 'penalty': penalties, 
                                 'class_weight': class_wgts,
                                 'weight_scheme': wgt_schemes,
                                 'round_dec': [0]
                                }
                 }


    log_lib = {'clf': LogisticRegression,
               'param_dict': {'C': c_list,
                                  'penalty': penalties,
                                  'class_weight': class_wgts
                             }
              }

    # specify list of k for precision at k
    k_list = [0.05, 0.1, 0.15, 0.2, 0.25, 0.3]

    # train a bunch of classifiers for each type of classifier
    # I wanted to find the best one of each, so I did each one separately
    expand_evals, expand_pkls = models.machine_learner(data_dct, 
                                                       clf_library=expand_wgt,
                                                       pkl_folder='e_pkls',
                                                       cv='kfold_cv',
                                                       k=k_list,
                                                       n_folds=10)

    simple_evals, simple_pkls = models.machine_learner(data_dct,
                                                       clf_library=simple_wgt,
                                                       pkl_folder='s_pkls',
                                                       cv='kfold_cv',
                                                       k=k_list,
                                                       n_folds=10)

    log_evals, log_pkls = models.machine_learner(data_dct,
                                                 clf_library=log_lib,
                                                 pkl_folder='log_pkls',
                                                 cv='kfold_cv',
                                                 k=k_list,
                                                 n_folds=10)

    # concatenate all the dataframes into one dataframe using
    # output of machine learner 
    expand_df = evaluation.dict_to_dataframe(expand_evals, expand_pkls)
    simple_df = evaluation.dict_to_dataframe(simple_evals, simple_pkls)
    log_df = evaluation.dict_to_dataframe(log_evals, log_pkls)

    # metric(s) to sort classifiers by 
    sort_metrics = ['precision at 0.1 mean', 'precision at 0.15 mean']
    # mapping between question number and text
    map_file = '/home/ipan/707G_question_map.csv'

    # get a dataframe with weights and question text
    expand_wgts = evaluation.weight_mapper(data_dct, expand_df, 
                                           sort_metrics, map_file, '707G')
    expand_wgts.columns = ['QID', 'Question', 'Expanded Weights']
    simple_wgts = evaluation.weight_mapper(data_dct, simple_df,
                                           sort_metrics, map_file, '707G')
    simple_wgts.columns = ['QID', 'Question', 'Simple Weights']
    log_wgts = evaluation.weight_mapper(data_dct, log_df, sort_metrics,
                                        map_file, '707G')
    
    all_wgts = log_wgts.join([expand_wgts['Expanded Weights'], 
                              simple_wgts['Simple Weights']])

    # load in models 
    log_df = log_df.sort(sort_metrics, ascending=False)
    log_model = joblib.load(log_df['pickle_file'][0])
    ew_model = joblib.load(expand_df.sort(sort_metrics, ascending=False)['pickle_file'][0])
    sw_model = joblib.load(simple_df.sort(sort_metrics, ascending=False)['pickle_file'][0])

    df = data_dct['dataframe']
    feats = data_dct['features']
    log_scores = log_model.predict_proba(df[feats])[:,1]
    pd.DataFrame({'scores': log_scores}).to_csv('scores.csv', index=False)
    # calculate overall rate of adverse births
    baseline_rate = np.round(df[data_dct['outcome']].mean()*100,1)

    # calculate scores 
    ew_scores = ew_model.predict_proba(df[feats])[:,1]
    sw_scores = sw_model.predict_proba(df[feats])[:,1]

    # get metrics for various values of k
    expand_mets = evaluation.metrics_getter(data_dct, expand_df,
                                             sort_metrics, map_file,
                                             k_list, ew_scores)
    simple_mets = evaluation.metrics_getter(data_dct, simple_df,
                                             sort_metrics, map_file, 
                                             k_list, sw_scores)
    log_mets = evaluation.metrics_getter(data_dct, log_df,
                                          sort_metrics, map_file,
                                          k_list, log_scores, scale=True)

    if not os.path.exists('best_pkl/'):
        os.makedirs('best_pkl/')

    # pickle the best logistic regression model for webapp prediction tool
    joblib.dump(log_model, 'best_pkl/best_model.pkl')

    return evaluation.weight_html(all_wgts), log_mets.to_html(), expand_mets.to_html(), simple_mets.to_html(), baseline_rate
