import numpy as np
import pandas as pd
from sklearn import metrics
import datetime
from dateutil.relativedelta import relativedelta
from itertools import izip_longest, product
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import BaggingClassifier
from sklearn.cross_validation import StratifiedKFold, KFold
from sklearn.externals import joblib
import os
import matplotlib.pyplot as plt
from scipy import interp
import style

def roc_writer(clf, y_true, y_prob, eval_folder, i=''):
    """
    This function will produce an ROC plot given a classifier's name or 
    the object itself, true labels and predicted scores for each fold, 
    and a folder to save it in. Can be used stand-alone. Supports single fold. 

    clf - string or classifier object
    y_true - list of numpy arrays, each array contains true labels
             for different folds  
    y_prob - list of numpy arrays, each array contains predicted scores
             for diffrent folds
    eval_folder - string, folder to save evaluations
                  images folder will be created in this folder containing 
                  the image 
    i - int, allows for function to be used with a list of classifiers 
        for unique names, is not required
    """
    # make sure that y_true, y_prob are iterable
    if type(y_true[0]) is not np.ndarray: y_true = [y_true]
    if type(y_prob[0]) is not np.ndarray: y_prob = [y_prob]

    # make an image folder if you have to
    img_dir = eval_folder+'/images/'
    if not os.path.exists(img_dir):
        os.makedirs(img_dir)

    # get the classifier family name i.e., LogisticRegression
    clf_name = str(clf)[:str(clf).index('(')]+str(i)

    enum_list = range(0, len(y_true))
    roc_auc = []

    mean_tpr = 0.0
    mean_fpr = np.linspace(0,1,1000)

    # plot each fold's ROC curve, calculate mean ROC curve
    for i in enum_list:
        fpr, tpr, _ = metrics.roc_curve(y_true[i], y_prob[i])
        mean_tpr += interp(mean_fpr, fpr, tpr) 
        mean_tpr[0] = 0.0
        roc_auc.append(metrics.roc_auc_score(y_true[i], y_prob[i]))
        plt.plot(fpr, tpr)

    mean_tpr /= len(y_true)
    mean_tpr[-1] = 1.0

    # make plot, plot mean ROC curve
    plt.title('Receiver Operating Characteristic')
    plt.plot(mean_fpr, mean_tpr, 'k--', label='Mean AUC = %0.2f\nSE AUC = %0.3f'% (np.mean(roc_auc), np.std(roc_auc)))
    plt.legend(loc='lower right')
    plt.plot([0,1],[0,1],'r--')
    plt.xlim([0,1.0])
    plt.ylim([0,1.0])
    plt.ylabel('True Positive Rate')
    plt.xlabel('False Positive Rate')

    plt.savefig(img_dir+'ROC_Curve_'+clf_name+'.png')

def pr_writer(clf, y_true, y_prob, eval_folder, i=''):
    """
    This function will produce a precision-recall plot given a classifier's 
    name or the object itself, true labels and predicted scores for each fold, 
    and a folder to save it in. Can be used stand-alone. Supports single fold. 

    clf - string or classifier object
    y_true - list of numpy arrays, each array contains true labels
             for different folds  
    y_prob - list of numpy arrays, each array contains predicted scores
             for diffrent folds
    eval_folder - string, folder to save evaluations
                  images folder will be created in this folder containing 
                  the image 
    i - int, allows for function to be used with a list of classifiers 
        for unique names, is not required
    """
    # make sure y_true, y_prob is iterable
    if type(y_true[0]) is not np.ndarray: y_true = [y_true]
    if type(y_prob[0]) is not np.ndarray: y_prob = [y_prob]

    img_dir = eval_folder+'/images/'
    if not os.path.exists(img_dir):
        os.makedirs(img_dir)

    clf_name = str(clf)[:str(clf).index('(')]+str(i)

    enum_list = range(0, len(y_true))
    pr_auc = []

    # plot pr each fold's PR curve
    for i in enum_list:
        prec, rec, _ = metrics.precision_recall_curve(y_true[i], y_prob[i])
        pr_auc.append(metrics.average_precision_score(y_true[i], y_prob[i]))
        plt.plot(rec, prec)

    # make plot, only show mean, SE for AUCs
    plt.title('Precision-Recall')
    plt.plot(rec, prec, linestyle='none', label='Mean AUC = %0.2f\nSE AUC = %0.3f'% (np.mean(pr_auc), np.std(pr_auc)))
    plt.legend(loc='upper right')
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.ylim([0.0, 1.05])
    plt.xlim([0.0, 1.0])


    plt.savefig(img_dir+'PR_Curve_'+clf_name+'.png')

def pratn_writer(clf, y_true, y_prob, eval_folder, i=''):
    """
    This function will produce a plot of precision & recall vs. percent 
    population given a classifier's name or the object itself, true labels 
    and predicted scores for each fold, and a folder to save it in. 
    Can be used stand-alone. Supports single fold. 

    clf - string or classifier object
    y_true - list of numpy arrays, each array contains true labels
             for different folds  
    y_prob - list of numpy arrays, each array contains predicted scores
             for diffrent folds
    eval_folder - string, folder to save evaluations
                  images folder will be created in this folder containing 
                  the image 
    i - int, allows for function to be used with a list of classifiers 
        for unique names, is not required
    """
    if type(y_true[0]) is not np.ndarray: y_true = [y_true]
    if type(y_prob[0]) is not np.ndarray: y_prob = [y_prob]

    img_dir = eval_folder+'/images/'
    if not os.path.exists(img_dir):
        os.makedirs(img_dir)

    clf_name = str(clf)[:str(clf).index('(')]+str(i)

    enum_list = range(0, len(y_true))

    fig, ax1 = plt.subplots()
    ax1.set_xlabel('percent of population')
    ax1.set_ylabel('precision', color='b')
    ax2 = ax1.twinx()
    ax2.set_ylabel('recall', color='r')

    mean_prec = 0.0
    mean_rec = 0.0
    mean_pct_above = np.linspace(0,1,1000)

    for i in enum_list:
        prec, rec, thres = metrics.precision_recall_curve(y_true[i], y_prob[i])
        prec = prec[:-1]
        rec = rec[:-1]
        skip_size = int(thres.shape[0]/1000.0)
        if skip_size == 0: skip_size = 1
	plotting_thres = thres[0::skip_size][::-1]
        plotting_prec = prec[0::skip_size][::-1]
        plotting_rec = rec[0::skip_size][::-1]

        how_many = float(len(y_true[i]))

        pct_above = [(y_prob[i][y_prob[i] >= value].shape[0])/how_many
                     for value in plotting_thres]

        pct_above = np.array(pct_above)
        mean_prec += interp(mean_pct_above, pct_above, plotting_prec)
        mean_rec += interp(mean_pct_above, pct_above, plotting_rec)

        #ax1.plot(pct_above, plotting_prec, 'b')
        #ax2.plot(pct_above, plotting_rec, 'r')

    mean_prec /= len(y_true)
    mean_rec /= len(y_true)

    mean_prec[-1] = np.mean([np.mean(enu) for enu in y_true])
    mean_rec[-1] = 1.0

    ax1.plot(mean_pct_above, mean_prec, 'b')
    ax2.plot(mean_pct_above, mean_rec, 'r')
    plt.title('Precision, Recall vs % Population')
    plt.savefig(img_dir+'PRATN_Curve_'+clf_name+'.png')

def md_writer(clf, features, outcome, eval_folder,
              config_file, summary_df, i=''):
    """
    This function writes an evaluation sheet to a markdown and HTML file.
    clf - classifier name or object 

    next 3 arguments are available from data_getter data dictionary output:
    features - list of features used 
    outcome - outcome
    config_file - path to config file 

    eval_folder - folder to save evals
    summary_df - output of one of the cross-validation functions 
                (dataframe of metrics)
    i - makes md_writer iterable, not required for single classifier 
    """
    if config_file.endswith('.xlsx'):
        config = pd.read_excel(config_file, sheetname='Sheet1')
    elif config_file.endswith('.csv'):
        config = pd.read_csv(config_file)

    clf_params = clf.get_params()
    clf_name = str(clf)[:str(clf).index('(')]
    clf_img = clf_name+str(i)

    file_name = clf_name+str(i)+'_Evaluation.md'

    save_file = open(eval_folder+file_name, 'w')

    def new_line():
        save_file.write('\n')

    save_file.write('<link rel="stylesheet" href="style.css" type="text/css" />\n')
    save_file.write('# Model Evaluation Report\n')
    new_line()

    save_file.write('## Data Configuration:\n')
    new_line()
    save_file.write(config.to_html(na_rep='', index=False).replace('NaT', ''))
    new_line()

    save_file.write('## Classifier Parameters: '+clf_name+'\n')
    new_line()
    for elem in clf_params:
        save_file.write('* {}: {}\n'.format(elem, clf_params[elem]))
    new_line()

    summary_df = summary_df.T
    summary_df.columns = ['value']

    save_file.write('## Evaluation Metrics; Summary\n')
    new_line()
    save_file.write(summary_df.to_html())
    new_line()

    save_file.write('## ROC Curve\n')
    new_line()
    save_file.write('![mis](images/ROC_Curve_'+clf_img+'.png)\n')
    new_line()

    save_file.write('## Precision-Recall Curve\n')
    new_line()
    save_file.write('![mis](images/PR_Curve_'+clf_img+'.png)\n')
    new_line()

    save_file.write('## Precision, Recall vs % Population\n')
    new_line()
    save_file.write('![mis](images/PRATN_Curve_'+clf_img+'.png)\n')

    if clf_name in ['LogisticRegression']:
        save_file.write('## Coefficients\n')
        new_line()
        for i,coef in enumerate(clf.coef_[0]):
            save_file.write('*<b>{}: {}</b>\n'.format(features[i], round(coef,4)))
        new_line()

    if clf_name in ['WeightedQuestions']:
        save_file.write('## Weights\n')
        new_line()
        for i,wt in enumerate(clf.weights):
            save_file.write('*<b>{}: {}</b>\n'.format(features[i], wt))
        new_line()

    save_file.close()

    def markdown_to_html(md_file, out_file_name=None):
        import markdown

        with open(md_file, 'r') as f:
            html = markdown.markdown(f.read())

        if out_file_name is None:
             out_file_name = md_file.split('.')[0]+'.html'
        with open(out_file_name, 'w') as f:
            f.write(html)

    markdown_to_html(eval_folder+file_name)

def generate_models(clf_library):

    """
    This function returns a list of classifiers with all combinations of
    hyperparameters specified in the dictionary of hyperparameter lists.
    usage example:
        lr_dict = {
                      'clf': LogisticRegression,
                      'param_dict': {
                           'C': [0.001, 0.1, 1, 10],
                           'penalty': ['l1', 'l2']
                           }
                  }
        sgd_dict = {
                       'clf': SGDClassifier,
                       'param_dict': {
                       'alpha': [0.0001, 0.001, 0.01, 0.1],
                       'penalty': ['l1', 'l2']
                       }
                   }
        clf_library = [lr_dict, sgd_dict]
        generate_models(clf_library)
    """
    clf_list = []
    for i in clf_library:
        param_dict = i['param_dict']
        dict_list = [dict(izip_longest(param_dict, v)) for v in product(*param_dict.values())]
        clf_list = clf_list+[i['clf'](**param_set) for param_set in dict_list]
    return clf_list

def precision_at_k(clf, train_x, y_true, y_score, k):

    if type(k) is not list: k = [k]

    if 'predict_proba' not in dir(clf):
       train_scores = clf.predict(train_x)
    else: train_scores = clf.predict_proba(train_x)[:,1]

    metrics_at_k_df = pd.DataFrame()
    prec_list = []
    rec_list = []
    test_ct_list = []
    test_percent_list = []

    for i in k:
        threshold = np.sort(train_scores)[::-1][int(i*len(train_scores))]
        y_pred = np.asarray([1 if i >= threshold else 0 for i in y_score])
        prec_list.append(metrics.precision_score(y_true, y_pred))
        rec_list.append(metrics.recall_score(y_true, y_pred))
        test_ct_list.append(y_pred.sum())
        test_percent_list.append(y_pred.mean())
    metrics_at_k_df['prec'] = prec_list
    metrics_at_k_df['rec'] = rec_list
    metrics_at_k_df['test_count'] = test_ct_list
    metrics_at_k_df['test_percent'] = test_percent_list
    metrics_at_k_df.index = [str(i) for i in k]

    return metrics_at_k_df

def partial_pr_auc(k, y_true, y_score):

    if type(k) is not list: k = [k]

    part_auc_df = pd.DataFrame()
    for i in k:
        threshold = sorted(y_score)[::-1][int(i*len(y_score))]
        precision, recall, thresholds = metrics.precision_recall_curve(y_true,
                                                                   y_score)
        precision, recall, thresholds = precision[::-1], recall[::-1], \
                                        thresholds[::-1]
        where_is = np.where(np.round(thresholds,5)==np.round(threshold,5))

        try:
            part_auc_df[str(i)]  = [metrics.auc(recall[:where_is[0][0]],
                                    precision[:where_is[0][0]],
                                    reorder=True)]
        except (ValueError, IndexError):
            part_auc_df[str(i)] = [0]


    return part_auc_df


def temporal_cv(clf, data_dct, first_train_end, train_date_var,
                test_date_var, increment, k,
                start_date=None, end_date=None, i=''):

    features = data_dct['features']
    outcome = data_dct['outcome']
    data = data_dct['dataframe'].dropna()

    if end_date == None:
        end_date = max(data[train_date_var])

    if start_date == None:
        start_date = min(data[test_date_var])

    date_bounds = []
    temp_end_date = first_train_end
    while temp_end_date < end_date:
        date_bounds.append(temp_end_date)
        temp_end_date += increment

    if date_bounds[-1] < end_date:
        date_bounds.append(end_date)

    prec_list = []
    recall_list = []
    train_size = []
    test_size = []
    test_count = []
    test_percent = []
    train_set_start = np.repeat(start_date, len(date_bounds)-1)
    test_set_start = []
    test_set_end = []

    for index, i in enumerate(date_bounds[:-1]):
        train_x = data.loc[(data[train_date_var] >= start_date) & (data[train_date_var] < i), features]
        train_Y = data.loc[(data[train_date_var] >= start_date) & (data[train_date_var] < i), outcome]
        test_x = data.loc[(data[test_date_var] >= i) & (data[test_date_var] < date_bounds[index+1]), features]
        test_Y = data.loc[(data[test_date_var] >= i) & (data[test_date_var] < date_bounds[index+1]), outcome]
        clf.fit(train_x, train_Y)
        if 'predict_proba' not in dir(clf):
            scores = clf.predict(test_x)
        else: scores = clf.predict_proba(test_x)[:,1]
        pak = precision_at_k(clf, train_x, test_Y, scores, k=k)
        train_size.append(len(train_Y))
        test_size.append(len(test_Y))
        test_set_start.append(i)
        test_set_end.append(date_bounds[index+1])

    summary_df = pd.DataFrame()
    summary_df['train_start'] = train_set_start
    summary_df['test_start'] = test_set_start
    summary_df['test_end'] = test_set_end
    summary_df['train_size'] = train_size
    summary_df['test_size'] = test_size
    for i in k:
        summary_df['precision at '+str(i)] = [metrics_at_k_df.loc[str(i),'prec']]
        summary_df['recall at ' +str(i)] = [metrics_at_k_df.loc[str(i), 'rec']]
        summary_df['test_count at '+str(i)] = [metrics_at_k_df.loc[str(i), 'test_count']]
        summary_df['test_percent at '+str(i)] = [metrics_at_k_df.loc[str(i), 'test_percent']]


    return summary_df

def train_test_splitter(data_dct, clf, test_size, k, i=''):
    data = data_dct['dataframe'].dropna().sort(data_dct['date'])
    features = data_dct['features']
    outcome = data_dct['outcome']

    final_train_index = int(len(data)*(1-test_size))
    train_x = np.array(data[features]).astype(float)[:final_train_index]
    train_Y = np.array(data[outcome]).astype(float)[:final_train_index]
    test_x = np.array(data[features]).astype(float)[final_train_index:]
    test_Y = np.array(data[outcome]).astype(float)[final_train_index:]

    clf.fit(train_x, train_Y)
    if 'predict_proba' not in dir(clf):
        scores = clf.predict(test_x)
    else: scores = clf.predict_proba(test_x)[:,1]
    summary_df = pd.DataFrame()
    summary_df['avg_precision'] = [metrics.average_precision_score(test_Y, scores)]
    summary_df['avg_prec_'+str(k)] = partial_pr_auc(k, test_Y, scores)
    summary_df['roc_auc'] = [metrics.roc_auc_score(test_Y, scores)]
    pak = precision_at_k(clf, train_x, test_Y, scores, k)
    for i in k:
        summary_df['precision at '+str(i)] = [metrics_at_k_df.loc[str(i),'prec']]
        summary_df['recall at ' +str(i)] = [metrics_at_k_df.loc[str(i), 'rec']]
        summary_df['test_count at '+str(i)] = [metrics_at_k_df.loc[str(i), 'test_count']]
        summary_df['test_percent at '+str(i)] = [metrics_at_k_df.loc[str(i), 'test_percent']]


    return summary_df

def kfold_cv(data_dct, clf, k, n_folds, eval_folder=None, stratified=True, i=''):
    if 'date' in data_dct.keys():
        data = data_dct['dataframe'].dropna().sort(data_dct['date'])
    else: data = data_dct['dataframe'].dropna()
    features = data_dct['features']
    outcome = data_dct['outcome']
    X = np.array(data[features]).astype(float)
    Y = np.array(data[outcome]).astype(float)

    summary_df = pd.DataFrame()
    avg_prec_list = []
    roc_auc_list = []

    if stratified:
        kf = StratifiedKFold(Y, n_folds=n_folds)
    else:
        kf = KFold(len(Y), n_folds=n_folds)

    all_scores = []
    all_y_true = []
    pak = pd.DataFrame(columns=['prec', 'rec', 'test_count', 'test_percent'])
    part_auc_df = pd.DataFrame(columns=[str(each_k) for each_k in k])
    for train,test in kf:
        train_x, train_Y = X[train], Y[train]
        test_x, test_Y = X[test], Y[test]
        clf.fit(train_x, train_Y)
        if 'predict_proba' not in dir(clf):
            scores = clf.predict(test_x)
        else: scores = clf.predict_proba(test_x)[:,1]
        all_scores.append(scores)
        all_y_true.append(test_Y)
        avg_prec_list.append(metrics.average_precision_score(test_Y,
                             scores))
        part_auc_df = part_auc_df.append(partial_pr_auc(k, test_Y, scores))
        roc_auc_list.append(metrics.roc_auc_score(test_Y, scores))
        pak = pak.append(precision_at_k(clf, train_x, test_Y, scores, k))


    if eval_folder is not None:
        lst_idx = range(0, len(all_scores))
        roc_writer(clf, all_y_true, all_scores, eval_folder, i=i)
        plt.clf()
        pr_writer(clf, all_y_true, all_scores, eval_folder, i=i)
        plt.clf()
        pratn_writer(clf, all_y_true, all_scores, eval_folder, i=i)
	plt.clf()

    summary_df['avg_prec_score_mean'] = [np.mean(avg_prec_list)]
    summary_df['avg_prec_score_std'] = [np.std(avg_prec_list)]
    summary_df['roc_auc_mean'] = [np.mean(roc_auc_list)]
    summary_df['roc_auc_std'] = [np.std(roc_auc_list)]
    for i in k:
        summary_df['avg_prec_'+str(i)+' mean'] = [part_auc_df.loc[:,str(i)].mean()]
        summary_df['avg_prec_'+str(i)+' std'] = [part_auc_df.loc[:,str(i)].std()]
        summary_df['precision at '+str(i)+' mean'] = [pak.loc[str(i),'prec'].mean()]
        summary_df['precision at '+str(i)+' std'] = [pak.loc[str(i),'prec'].std()]
        summary_df['recall at ' +str(i)+' mean'] = [pak.loc[str(i), 'rec'].mean()]
        summary_df['recall at ' +str(i)+' std'] = [pak.loc[str(i), 'rec'].std()]
        summary_df['test_count at '+str(i)+' mean'] = [pak.loc[str(i), 'test_count'].mean()]
        summary_df['test_count at '+str(i)+' std'] = [pak.loc[str(i), 'test_count'].std()]
        summary_df['test_percent at '+str(i)+' mean'] = [pak.loc[str(i), 'test_percent'].mean()]
        summary_df['test_percent at '+str(i)+' std'] = [pak.loc[str(i), 'test_percent'].std()]

    return summary_df



def machine_learner(data_dct,
                    clf_library,
                    cv,
                    make_evals=False,
                    pkl_folder='pickles',
                    verbose=False,
                    eval_folder='evals',
                    n_folds=5,
                    stratified=True,
                    bagging=False,
                    test_size=0.3,
                    increment=6,
                    k=0.1,
                    start_date=None,
                    end_date=None,
                    **kwargs):

    """
    This function is customized for temporal cross-validation and
    precision-at-k evaluation.
    data_dct - dictionary containing data, features, outcome from
               features.data_getter()
    clf_library - list of classifier dictionaries in following format
            {
                'clf': Classifier(),
                'param_dict': {
                    'param1': [list of parameter values],
                    'param2': [list of parameter values]
                }
            }
    cv - specify type of cross-validation as string, current options are:
            'temporal_cv', 'train_test_split', 'kfold_cv'
    pkl_folder - folder to save pickle files
    n_folds - if using kfold_cv, number of folds to use
    stratified - True/False, whether to use stratified kfold cv
    test_size - if using train_test_split, size of test set as decimal
                i.e., 0.1, 0.2, 0.3
    first_train_end - if using temporal_cv, specify the end date of the
                      initial training set
    train_date_var - if using temporal cv, which date variable to use for
                     train set
        format: 'Year-Month-Day' (include quotes)
    test_date_var - if using temporal cv, which date variable to use for
                    test set
        format: 'Year-Month-Day' (include quotes)
    *** PLEASE NOTE: most of the time these will be the same.  In our case,
        we must look at birth date for the train set and assessment date
        for the test set because we can't train on data without seeing the
        birth outcome first but we can predict on data without the birth
        outcome. All observations in our dataset have realized birth outcomes
        at some point in time
    increment - time window of the training set specified in months
                (default: 6, aka 6 months)
    k - precision at what top fraction of data (default: 0.1, aka top 10%)
    *** PLEASE NOTE: the current implementation of precision @ k uses
        looks at the top k fraction of the TRAINING SET to determine the
        threshold for the TEST set, which is crucial for our purposes ***

    start_date - date of first observation to include
        format: 'Year-Month-Day' (include quotes)
        (default: min(train_date_var))
    end_date - date of last observation to include
        format: 'Year-Month-Day' (include quotes)
        (default: max(train_date_var))
    """

    # get start time for runtime calculation
    total_start_time = datetime.datetime.now()

    # if no folder specified for pickle files, ask for confirmation of default
    if pkl_folder == 'pickles':
        verify = raw_input(('You have not specified a folder for the '
                            'pickle files. By default, they will be saved '
                            'in ./pickles/ - do you want to continue? (y/n)'))
        if verify.lower() == 'n':
            print(('Please specify a folder using the "pkl_folder" argument of '
                   'machine_learner'))
            return 0

    # if user wants evals and does not specify folder, ask for confirmation
    # of default
    if make_evals:
        if eval_folder == 'evals':
            verify = raw_input(('You have not specified a folder for the '
                                'evaluation files. By default, they will be '
                                'saved in ./evals/ - do you want to continue? '
                                '(y/n)'))
            if verify.lower() == 'n':
                print(('Please specify a folder using the "eval_folder" '
                       'argument of machine_learner'))
                return 0

    else: eval_folder = None

    # make sure clf_library is iterable (a list)
    if type(clf_library) is not list: clf_library = [clf_library]

    train_date_var = kwargs.get('train_date_var', None)
    test_date_var = kwargs.get('test_date_var', None)

    # generate list of instantiated objects from list of dictionaries
    clf_library = generate_models(clf_library)
    if bagging: # add bagging if specified
        bag_lib = [BaggingClassifier(base_estimator=i, n_estimators=100)
                   for i in clf_library]
        clf_library = clf_library+bag_lib

    # convert dates to right format if using temporal_cv
    if cv == 'temporal_cv':
        increment = relativedelta(months=+increment)
        date_pat = '%Y-%m-%d'
        first_train_end = datetime.datetime.strptime(first_train_end, date_pat).\
                                                                         date()
        if start_date is not None:
            start_date = datetime.datetime.strptime(start_date, date_pat).date()

        if end_date is not None:
            end_date = datetime.datetime.strptime(end_date, date_pat).date()

    # make folder for pickle files if it doesn't exist
    if pkl_folder[-1] != '/':
        pkl_folder += '/'
    if not os.path.exists(pkl_folder):
        os.makedirs(pkl_folder)

    # make folder for eval files if it doesn't exist
    if make_evals:
        if eval_folder[-1] != '/':
            eval_folder += '/'
        if not os.path.exists(eval_folder):
            os.makedirs(eval_folder)

    evaluate_dct = {}
    pickle_dct = {}
    df = data_dct['dataframe'].dropna()
    feats = data_dct['features']
    outc = data_dct['outcome']
    if 'config_file' in data_dct.keys():
        config_file = data_dct['config_file']

    for i, clf in enumerate(clf_library):
        if verbose:
            print('Running '+str(clf)+'\n...')
            start_time = datetime.datetime.now()
        if cv == 'temporal_cv':
            evaluate_dct[str(clf)] = temporal_cv(clf, data_dct, first_train_end,
                                                 train_date_var, test_date_var,
                                                 increment, k,
                                                 start_date, end_date, i=i)

        elif cv == 'train_test_split':
            evaluate_dct[str(clf)] = train_test_splitter(data_dct, clf,
                                                         test_size, k, i=i)

        elif cv == 'kfold_cv':
            evaluate_dct[str(clf)] = kfold_cv(data_dct, clf, k, n_folds,
                                              eval_folder, stratified, i=i)

        clf_name = str(clf)[:str(clf).index('(')]
        pkl_file_path = pkl_folder+clf_name+str(i)+'.pkl'
        clf.fit(df[feats], df[outc])
        joblib.dump(clf, pkl_file_path,
                    compress=3) # dump pickle file
        pickle_dct[str(clf)] = pkl_file_path
        if make_evals: md_writer(clf, feats, outc, eval_folder,
                                 config_file, evaluate_dct[str(clf)], i=i)
        if verbose: print('Finished in: '+str(datetime.datetime.now()-
                                              start_time)+'\n')

    if make_evals:
        with open(eval_folder+'style.css', 'w') as f:
            f.write(style.make_style_sheet())

    print('machine_learner: finished running models')
    print('machine_learner: pickle files available in ' + pkl_folder)
    total_rtime = str(datetime.datetime.now()-total_start_time)
    if make_evals:
        print ('machine_learner: pickle files available in ' + eval_folder)
    print('machine_learner: total runtime was '+total_rtime)
    return evaluate_dct, pickle_dct

class WeightedQuestions(LogisticRegression):

    """
    This class takes coefficients generated from LogisticRegression and
    transforms them into weights based on various weighting schemes defined
    as methods in WeightedQuestions.
    Can be initialized with same parameters as LogisticRegression in addition
    to:
    weight_scheme - string, current methods include 'odds_ratio_absolute',
                            'odds_ratio_relative', 'positive_coefs'
        default: 'positive_coefs'
    round_dec - int, number of decimal places to round weights
        default: 0 (round to integer)
    """

    def __init__(self, penalty='l2', dual=False, tol=1e-4, C=1.0,
                 fit_intercept=True, intercept_scaling=1, class_weight=None,
                 random_state=None, solver='liblinear', max_iter=100,
                 multi_class='ovr', verbose=0, weight_scheme='positive_coefs',
                 round_dec=0, custom_weights=None):

        super(WeightedQuestions, self).__init__(penalty, dual, tol, C,
                                                fit_intercept,
                                                intercept_scaling,
                                                class_weight, random_state,
                                                solver, max_iter, multi_class,
                                                verbose)
        if weight_scheme not in ['odds_ratio_absolute',
                                 'odds_ratio_relative',
                                 'positive_coefs',
                                 'lin_reg_coefs',
                                 'marginal_effects',
                                 'no_change']:
            raise Exception(('weight_scheme '+weight_scheme+' is not valid. '
                             'Please enter a valid weight_scheme.'))

        self.weight_scheme = weight_scheme
        self.round_dec = round_dec
        self.custom_weights = custom_weights
        if type(self.custom_weights) in (int, float):
            error = ('Please make sure you specify a list of lists when '
                     'passing arguments into custom_weights when '
                     'specifying them in the param_dict.')
            raise Exception(error)

    def odds_ratio_absolute(self):
        weights = np.round(np.exp(self.coefs), self.round_dec)
        weights = [w if w > 0 else 1 for w in weights]
        return weights

    def odds_ratio_relative(self):
        return np.round(np.exp(self.coefs)/min(np.exp(self.coefs)),
                        self.round_dec)

    def positive_coefs(self):
        return np.round(self.coefs+abs(min(0,min(self.coefs)))+1,
                        self.round_dec)

    def no_change(self):
        return np.round(self.coefs*10, self.round_dec)

    def lin_reg_coefs(self, x):
        from sklearn.linear_model import LinearRegression
        log_odds = np.dot(x, self.coefs)
        probs = np.exp(log_odds)/(1+np.exp(log_odds))
        lin_reg = LinearRegression()
        lin_reg.fit(x, probs)
        return np.round(lin_reg.coef_+abs(min(0,min(lin_reg.coef_)))+1,
                        self.round_dec)

    def marginal_effects(self, x):
        x = np.array(x).astype(float)

        def expit(x):
            return np.exp(x)/(1+np.exp(x))

        log_odds = np.dot(x, self.coefs)
        probs = expit(log_odds)
        marg_eff = []

        for col in range(0, x.shape[1]):

            x_temp = x
            x_temp[:,col] = np.repeat(0, len(x_temp))
            log_odds0 = np.dot(x_temp, self.coefs)
            probs0 = expit(log_odds0)

            x_temp = x
            x_temp[:,col] = np.repeat(1, len(x_temp))
            log_odds1 = np.dot(x_temp, self.coefs)
            probs1 = expit(log_odds1)

            marg_eff.append(np.mean(probs1-probs0))

        marg_eff = np.array(marg_eff)
        self.marg_eff = marg_eff

        return np.round((marg_eff+abs(min(0,min(marg_eff))))*10+1,
                        self.round_dec)

    def fit(self, x,y):

        self.coefs = super(WeightedQuestions, self).fit(x,y).coef_[0]

        if self.weight_scheme == 'odds_ratio_absolute':
            weight_function = self.odds_ratio_absolute
        elif self.weight_scheme == 'odds_ratio_relative':
            weight_function = self.odds_ratio_relative
        elif self.weight_scheme == 'positive_coefs':
            weight_function = self.positive_coefs
        elif self.weight_scheme == 'lin_reg_coefs':
            weight_function = self.lin_reg_coefs
        elif self.weight_scheme == 'marginal_effects':
            weight_function = self.marginal_effects
        elif self.weight_scheme == 'no_change':
            weight_function = self.no_change

        if self.weight_scheme in ['marginal_effects', 'lin_reg_coefs']:
            self.weights = weight_function(x)
        else:
            self.weights = weight_function()

        if self.custom_weights is not None:
            print("WeightedQuestions: using custom weights...")
            self.weights = self.custom_weights

    def predict_proba(self, x):
        return np.vstack([np.dot(x, self.coefs), np.dot(x, self.weights)]).T
