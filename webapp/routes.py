from flask import Flask, render_template, request, redirect, url_for
from flask.ext.httpauth import HTTPDigestAuth
from celery import Celery
import proto
import os 
import sqlalchemy
import subprocess
import shutil
import numpy as np
import pandas as pd 
from sklearn.externals import joblib
from scipy.stats import percentileofscore

# subclass FlaskApp 
class FlaskApp(Flask):
    def __init__(self, *args, **kwargs):
        super(FlaskApp, self).__init__(*args, **kwargs)
        self.model = joblib.load('best_pkl/best_model.pkl')

app = FlaskApp(__name__)      
app.config['CELERY_BROKER_URL'] = 'amqp://guest:guest@localhost:5672//'
app.config['CELERY_RESULT_BACKEND'] = 'db+'+open('/mnt/data/predicting-adverse-births/passwords/psql_engine.password', 'r').read()
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

app.config['SECRET_KEY'] = # insert some key here
auth = HTTPDigestAuth()

users = {
    'user1': 'pw1',
    'user2': 'pw2'
}

@celery.task
def my_task():
    if os.path.exists('finished.txt'):
        os.remove('finished.txt')

    if os.path.exists('templates/weights.html'):
        os.remove('templates/weights.html')
    
    wgts, lmets, emets, smets, rate  = proto.run_all()
    pre_string = ('{% extends "layout_admin.html" %}\n'
                  '{% block content %}\n'
                  '<div class="row">\n'
                  '<div class="col-sm-10 col-centered">\n'
                  '<h1 align="center">Question Weights</h1>\n')

    old_table = '<table border="1" class="dataframe">'
    new_table = '<table class="table table-striped table-condensed">'

    with open('templates/weights.html', 'w') as f:    
        f.write(pre_string+wgts.replace(old_table, new_table)+
                '\n</div></div>\n'+
                '<div class="row">\n<div class="col-sm-6 col-centered">\n'+
                '<br>\n<h3 align="center"><b>'+str(rate)+'%</b> of women who have '+
                'previously taken the 707G had adverse births.</h3>\n<br>\n'+
                '\n<h1 align="center">Metrics</h1>\n<br>\n'+
                '<h2 align="center">Model Score</h2>\n'+
                lmets.replace(old_table, new_table)+
                '\n<br>\n<h2 align="center">Expanded Weights</h2>\n'+
                emets.replace(old_table, new_table)+
                '\n<br>\n<h2 align="center">Simple Weights</h2>\n'+
                smets.replace(old_table, new_table)+
                '\n</div>\n</div>\n{% endblock %}')
    
    if os.path.exists('templates/metrics.html'):
        os.remove('templates/metrics.html')

    mpre_string = ('<div class="row">\n'
                   '<div class="col-sm-6 col-centered">\n'
                   '<h2 align="center">Model Score</h2>\n')

    with open('templates/metrics.html', 'w') as m:
        m.write(mpre_string+lmets.replace(old_table, new_table)+
                '\n</div>\n</div>\n')

    if os.path.exists('templates/weights_archive.html'):
        os.remove('templates/weights_archive.html')

    if os.path.exists('templates/metrics_archive.html'):
        os.remove('templates/metrics_archive.html')
    
    shutil.copy('templates/weights.html', 'templates/weights_archive.html')
    shutil.copy('templates/metrics.html', 'templates/metrics_archive.html')
    
    with open('finished.txt', 'w') as f:
        f.write('done')

@auth.get_password
def get_pw(username):
    if username in users:
        return users.get(username)
    return None

@app.route('/')
def home():
    return render_template('landing.html')

@app.route('/login', methods=['GET', 'POST'])
@auth.login_required
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] != 'someuser' or request.form['password'] != 'somepw':
            error = 'Invalid credentials. Please try again.'
        else:
            return redirect(url_for('admin'))
    return render_template('login.html', error=error)

@app.route('/admin')
@auth.login_required
def admin():
    return render_template('admin.html')

@app.route('/clinic')
@auth.login_required
def clinic():
    return render_template('clinic.html')

@app.route('/preweights')
@auth.login_required
def preweights():
    my_task.apply_async()
    return render_template('weights_requested.html')

@app.route('/weights')
@auth.login_required
def weights():
    if not os.path.exists('finished.txt'):
        return render_template('weights_loading.html')
    else:
        return render_template('weights.html')

@app.route('/weights_archive')
@auth.login_required
def weights_archive():
    return render_template('weights_archive.html')

@app.route('/warning')
@auth.login_required
def warning():
    return render_template('warning.html')

@app.route('/update')
@auth.login_required
def update():
    return 0

@app.route('/predict', methods=['GET', 'POST'])
@auth.login_required
def predict():
    if request.method == 'GET':
        return render_template('predict.html')
    else:
        answers = []
        for i in range(1,18):
            if '1' in request.form.getlist('q'+str(i)):
                answers.append(1)
            else:
                answers.append(0)
        
        answers = np.array(answers)
        if sum(answers) > 1:
            answers_num = list(np.where(answers==1)[0]+1)
            save_answers = 'You checked questions ' + \
                           ', '.join([str(x) for x in answers_num[:-1]]) + \
                           ' and '+ str(answers_num[-1]) + '.'
        elif sum(answers) == 1:
            answers_num = list(np.where(answers==1)[0]+1)
            save_answers = 'You checked question ' + str(answers_num[0]) + '.' 
        else: 
            save_answers = 'You did not check any questions.'

        risk_score = app.model.predict_proba(answers)[0][1]
        threshold = pd.read_csv('threshold.csv')['threshold'][0]
        if risk_score*100 >= threshold:
            danger = True
        else: 
            danger = None
        scores = np.array(pd.read_csv('scores.csv')['scores'])
        percentile = int(100-np.round(percentileofscore(scores, risk_score)))
        risk_bar = int(min(100, (risk_score*100)/threshold*50))
        return render_template('predict.html', 
                               risk_score=str(np.round(risk_score*100,2)),
                               threshold=str(threshold),
                               danger=danger,
                               risk_bar=str(risk_bar),
                               my_answers=save_answers,
                               percentile=str(max(0.1,percentile)))
        # return str(answers)+'\n'+str(risk_score)

@app.route('/threshold', methods=['GET', 'POST'])
@auth.login_required
def threshold():
    is_set = None
    pre = None
    all_scores = np.array(pd.read_csv('scores.csv')['scores'])*100
    if request.method == 'POST':
        score = request.form['minscore']
        is_set = True
        pd.DataFrame({'threshold': [score]}).to_csv('threshold.csv', 
                                                    index=False)
        percentile = int(100-np.round(percentileofscore(all_scores, score)))
        set_pct = str(percentile)+'%'
        return render_template('threshold.html', is_set=is_set, 
                                score=score, set_pct=set_pct)
    if os.path.exists('threshold.csv'):
        pre = pd.read_csv('threshold.csv')['threshold'][0]
        percentile = int(100-np.round(percentileofscore(all_scores, pre)))
        pre_pct = str(percentile)+'%'
    return render_template('threshold.html', pre=pre, pre_pct=pre_pct)

@app.route('/map')
def map():
    return render_template('map.html')

@app.route('/map_admin')
@auth.login_required
def map_admin():
    return render_template('map.html', admin=True)

@app.route('/map_clinic')
@auth.login_required
def map_clinic():
    return render_template('map.html', clinic=True)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
