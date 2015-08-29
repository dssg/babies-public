import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import markdown
from sklearn import metrics
from sklearn.externals import joblib
import re

def plot_precision_recall_n(y_true, y_prob, model_name=None):
    # thanks rayid
    from sklearn.metrics import precision_recall_curve
    y_score = y_prob
    precision_curve, recall_curve, pr_thresholds = precision_recall_curve(y_true, y_score)
    precision_curve = precision_curve[:-1]
    recall_curve = recall_curve[:-1]
    pct_above_per_thresh = []
    number_scored = len(y_score)
    for value in pr_thresholds:
      num_above_thresh = len(y_score[y_score>=value])
      pct_above_thresh = num_above_thresh / float(number_scored)
      pct_above_per_thresh.append(pct_above_thresh)
    pct_above_per_thresh = np.array(pct_above_per_thresh)
    plt.clf()
    fig, ax1 = plt.subplots()
    ax1.plot(pct_above_per_thresh, precision_curve, 'b')
    ax1.set_xlabel('percent of population')
    ax1.set_ylabel('precision', color='b')
    ax2 = ax1.twinx()
    ax2.plot(pct_above_per_thresh, recall_curve, 'r')
    ax2.set_ylabel('recall', color='r')

    if model_name is not None:
        name = model_name
        plt.title(name)
    #plt.savefig(name)
    plt.show()

def current_strat(outcome, conn):
    sql_outcome = '"' + outcome + '_OTC"'

    qts707g2 = range(35, 52)
    col_names = ['707G_'+str(float(i))+'_Q' for i in qts707g2]
    qts707g2 = ['"'+i+'"' for i in col_names]

    query = 'SELECT "UNI_PART_ID_I", ' + ','.join(qts707g2) + ',' + \
            sql_outcome + ',"BBO_F" FROM core_birth_info_rc WHERE \
            "707G_LT_D" >= \'2014-07-01\' AND ' + sql_outcome + ' IS NOT NULL'

    df707g2 = pd.read_sql(query, conn)

    points = [1,1,1,1,1,1,1,1,2,1,2,1,1,1,1,1,1]
    scores = np.dot(df707g2[col_names], points)
    df707g2['elig'] = [1 if i >= 2 else 0 for i in scores]

    results_dct = {}
    results_dct['precision'] = metrics.precision_score(df707g2[outcome+'_OTC'],
                                                       df707g2['elig'])
    results_dct['recall'] = metrics.recall_score(df707g2[outcome+'_OTC'],
                                                 df707g2['elig'])
    results_dct['prior'] = df707g2[outcome+'_OTC'].mean()
    results_dct['bbo_crosstab'] = pd.crosstab(df707g2['BBO_F'],
                                              df707g2['elig'], margins=True)
    results_dct['graph'] = plot_precision_recall_n(df707g2[outcome+'_OTC'],
                                                   scores,
                                                   'Precision, Recall vs % Eligible')
    return results_dct

def dict_to_dataframe(eval_dct, pkl_dct):
    df = pd.DataFrame(columns=eval_dct[eval_dct.keys()[0]].columns.values)
    for key in eval_dct.keys():
        eval_dct[key].index = [key]
        df = df.append(eval_dct[key])
    pkl_df = pd.DataFrame({'index': pkl_dct.keys(),
                           'pickle_file': pkl_dct.values()}).set_index('index')
    return df.join(pkl_df)

def markdown_to_html(md_file, out_file_name=None):
    input_file = open(md_file, 'r')
    text = input_file.read()
    html_file = markdown.markdown(text)

    if out_file_name is None:
        out_file_name = md_file.split('.')[0]+'.html'

    out_file = open(out_file_name, 'w')
    out_file.write(html_file)

    input_file.close()
    out_file.close()

    return 'Your converted HTML file is saved as ' + out_file_name

def weight_mapper(data_dct, eval_df, sort_list, mapping, assmnt):
    if type(assmnt) is not str:
        assmnt = str(assmnt)

    eval_df.sort(sort_list, inplace=True, ascending=False)

    model = joblib.load(eval_df['pickle_file'][0])

    if 'weights' in model.__dict__:
        wts = model.weights
    else:
        wts = model.coef_[0]

    mapping = pd.read_csv(mapping)

    config = pd.read_csv(data_dct['config_file'])

    questions = [q for q in data_dct['features']
                 if bool(re.search(r'(?i)_Q$', q))]

    mapping.loc[:,'QUESTION_N'] = [assmnt+'_'+str(float(i))+'_Q'
                                   for i in mapping['QUESTION_N']]

    mapping_sub = mapping[[True if i in questions else False for i in mapping['QUESTION_N']]]
    mapping_sub.loc[:,'weights'] = wts

    return mapping_sub

def weight_html(df):
    df.columns = ['QID', 'Question', 'Model Score', 'Expanded Weights', 'Simple Weights']
    df = df.set_index('QID')
    df.index.name =  None
    return df.to_html()

def metrics_getter(data_dct, eval_df, sort_list, mapping, k, scores,
                   rnd=True, scale=False):

    eval_df.sort(sort_list, inplace=True, ascending=False)
    reg_ex = r'precision at.*mean|test_percent at.*mean'
    metric_cols = eval_df.columns.str.contains(reg_ex)
    metric_df = pd.DataFrame(eval_df.iloc[0, metric_cols])

    prec_index = metric_df.index.str.contains(r'precision')
    test_index = metric_df.index.str.contains(r'test_percent')

    prec = metric_df.iloc[prec_index,:].reset_index()
    test = metric_df.iloc[test_index,:].reset_index()

    mdf = pd.DataFrame({'Precision': prec.iloc[:,1],
                        'Predicted % Eligible': test.iloc[:,1]
                       })
    mdf.index = ['Top '+str(int(each_k*100))+'%' for each_k in k]

    mdf = mdf.astype(float)

    if rnd:
        fmat = lambda x: str(np.round(x,1))+'%'
        mdf = (mdf*100).applymap(fmat)

    scores = sorted(scores)[::-1]

    mes = 'Minimum Eligibility Score'
    if scale:
        mdf[mes] = [np.round(scores[int(each_k*len(scores))]*100,2)
                    for each_k in k]
    else: mdf[mes] = [scores[int(each_k*len(scores))] for each_k in k]

    return mdf
