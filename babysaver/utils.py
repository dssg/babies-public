def weight_getter(clf, features, df):
    clf_name = str(clf)[:str(clf).index('(')]
    try:
        wts = clf.feature_importances_ 
        typ = 'Importance'
        wts = pd.DataFrame({'Type': np.repeat(typ, len(wts)), 'Feature': features, 'Value': wts}).sort('Value', ascending=False)
        wts.index = np.repeat(str(clf), len(wts))
        df = df.append(wts)
    except (AttributeError, ValueError) as e:
        print(clf_name + ' does not have feature importances')
    try:
        wts = clf.coef_[0]
        typ = 'Effect Size'
        wts = pd.DataFrame({'Type': np.repeat(typ, len(wts)), 'Feature': features, 'Value': wts}).sort('Value', ascending=False)
        wts.index = np.repeat(str(clf), len(wts))
        df = df.append(wts)
    except (AttributeError, ValueError) as e:
        print(clf_name + ' does not have coefficients')

    return df

def question_weighter(data, weight_mat):
    data = data.dropna()
    weight_mat = weight_mat[weight_mat['Type'] == 'Effect Size']
    scores = np.dot(np.array(data[list(weight_mat['Feature'])]), np.round(weight_mat['Value']+weight_mat['Value'].min()+1,0))
    return scores