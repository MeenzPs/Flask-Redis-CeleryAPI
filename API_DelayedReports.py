# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22 14:09:55 2020

@author: EILAP6639
"""


import pymysql
from flask import Flask,request,abort, make_response, jsonify,json
import db_config
import psycopg2
import pandas as pd
import report1
import report2, report3
from celery import Celery, task
from datetime import datetime
from numbers import Number
import json



def make_celery(app):
    celery = Celery(app.import_name, backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery


app = Flask(__name__)
# json = FlaskJSON(app)
app.config.update(
    CELERY_BROKER_URL='redis://localhost:6379', #Default Port for Redis
    CELERY_RESULT_BACKEND='redis://localhost:6379', #Default Port for Redis
    # import modules to tasks
    CELERY_IMPORTS=("report1","report2","report3")
)

celery = make_celery(app)
#student usage report task is initiated
@celery.task(bind=True, name="report1")
def report1(self,d):
    self.d=d[0]
    report1.func1(self.d)
    print("Report 1 generated and uploaded on S3")    
@celery.task(bind=True, name="report2")
def report2(self,d,rep):
    self.d=d[0]
    self.rep=rep
    report2.func2(self.d,self.rep)
    print("Report 2 generated and uploaded on S3")    
#Mindspark Performance Report Download task is initiated
@celery.task(bind=True, name="report3")
def report3(self,d):
    self.d=d[0]
    report3.func3(self.d)
    print("Report 3 generated and uploaded on S3") 


@app.route('/report1',methods=['POST'])
def report1():
    headers = request.headers
    auth = headers.get("###") #API Secret Key
    if auth == '****': #API Secret KeyValue
        
        conn = None;
        cursor = None;
        # global d
        a={}
        try:
            a=request.get_json()             
            if check condition 1 fails:
                abort(make_response(jsonify(error_code='ERROR',message="ERROR MESSAGE"), 400))
            conn =psycopg2.connect(dbname=db_config.redshift_dbname, host=db_config.redshift_host, port=db_config.redshift_port, user=db_config.redshift_user, password=db_config.redshift_password)
            conn.autocommit = True
            cur = conn.cursor();
            entry_values_a=str(tuple(a.values()))
            cur.execute("""insert into tablename (col1,col2, col3) VALUES %s """%entry_values_a )
            a1 = [a]
            #Delayed report activation
            report1.delay(a1)
            return jsonify(status_code='Success Code',message='Success')
        except (KeyError, TypeError, ValueError) as e:
            return jsonify(e)
    else:
        return jsonify({"message": "ERROR: Unauthorized"}), 401

@app.route('/report2',methods=['POST'])
def report2():
    headers = request.headers
    auth = headers.get("###") #API Secret Key
    if auth == '****': #API Secret KeyValue
        
        conn = None;
        cursor = None;
        # global d
        b={}
        try:
            b=request.get_json()
                    
            if check condition 2 fails:
                abort(make_response(jsonify(error_code='ERROR',message="ERROR MESSAGE"), 400))
            conn =psycopg2.connect(dbname=db_config.redshift_dbname, host=db_config.redshift_host, port=db_config.redshift_port, user=db_config.redshift_user, password=db_config.redshift_password)
            conn.autocommit = True
            cur = conn.cursor();
            entry_values_b=str(tuple(b.values()))
            cur.execute("""insert into tablename (col1,col2, col3) VALUES %s """%entry_values_b)
            b1 = [b]
            #Delayed report activation
            report2.delay(b1)
            return jsonify(status_code='Success Code',message='Success')
        except (KeyError, TypeError, ValueError) as e:
            return jsonify(e)
    else:
        return jsonify({"message": "ERROR: Unauthorized"}), 401

@app.route('/report3',methods=['POST'])
def report3():
    headers = request.headers
    auth = headers.get("###") #API Secret Key
    if auth == '****': #API Secret KeyValue
        
        conn = None;
        cursor = None;
        # global d
        a={}
        try:
            a=request.get_json()
            print(a['reportid'])
                
                
            if check condition 1 fails:
                abort(make_response(jsonify(error_code='ERROR',message="ERROR MESSAGE"), 400))
            conn =psycopg2.connect(dbname=db_config.redshift_dbname, host=db_config.redshift_host, port=db_config.redshift_port, user=db_config.redshift_user, password=db_config.redshift_password)
            conn.autocommit = True
            cur = conn.cursor();
            entry_values_a=str(tuple(a.values()))
            cur.execute("""insert into tablename (col1,col2, col3) VALUES %s """%entry_values_a )
            a1 = [a]
            #Delayed report activation
            report3.delay(a1)
            return jsonify(status_code='Success Code',message='Success')
        except (KeyError, TypeError, ValueError) as e:
            return jsonify(e)
    else:
        return jsonify({"message": "ERROR: Unauthorized"}), 401



if __name__=='__main__':
    app.run(host='0.0.0.0', port='5000',debug=True)
