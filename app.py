#Name: Rushikesh Mahesh Bhagat
#UTA ID: 1001911486

from flask import Flask, flash, render_template, request, redirect

import pyodbc
import csv

from datetime import datetime
import haversine as hs
import requests

from settings_template import server, database, username, password, driver, mapQuest_key, mapQuest_url

connstr = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password

app = Flask(__name__)

def upload_csv(filename): 
    try:
        conn = pyodbc.connect(connstr)
        #print("conn",conn)
        cursor = conn.cursor()
        #print("cursor",cursor)
        path = './'
        count = 0
        table = 'rb'
        with open (filename, 'r') as file:
            
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                #row.pop()
                row = ['NULL' if val == '' or val == '-1' else val for val in row]
                row = [x.replace("'", "''") for x in row]
                out = "'" + "', '".join(str(item) for item in row) + "'"
                out = out.replace("'NULL'", 'NULL')
                query = "INSERT INTO " + table + " VALUES (" + out + ")"
                #print("query",query)
                cursor.execute(query)
                count+=1
            cursor.commit()
    
    except Exception as e:
        print(e,"Error connecting DB")

    finally:
        if conn:
            cursor.close()
            conn.close()
    print("Added " + str(count) + " rows into table " + table)



import os
import sys
from whoosh.index import create_in 
from whoosh.fields import Schema, TEXT, ID
from whoosh.analysis import StemmingAnalyzer

my_analyzer = StemmingAnalyzer() 

def read_text_docs(root_dir):   
    schema = Schema(title=TEXT(stored=True),path=ID(stored=True), content=TEXT(analyzer=my_analyzer), textdata=TEXT(stored=True))
    if not os.path.exists("indexdir"):
        os.mkdir("indexdir")
        
    index_sch = create_in("indexdir",schema)
    writer = index_sch.writer()
    
    filepaths = [os.path.join(root_dir,i) for i in os.listdir(root_dir)]
    for filepath in filepaths:
        fp = open(filepath,'r',encoding='utf-8')
        print(filepath)
        text = fp.read()
        writer.add_document(title=filepath.split("\\")[1], path=filepath,content=text, textdata=text)
        fp.close()        
    writer.commit()

root_dir = "SearchDocuments"
read_text_docs(root_dir)

from whoosh.index import open_dir
from whoosh.qparser import QueryParser
from whoosh import scoring,highlight,qparser

def search_entered_query(query_str):
    index_sch = open_dir("indexdir")
    list_result51 = []
    with index_sch.searcher(weighting=scoring.BM25F) as searcher:
            query_parser = qparser.QueryParser("content", index_sch.schema)
            query_parser.add_plugin(qparser.FuzzyTermPlugin())
            query = query_parser.parse(query_str)
            results = searcher.search(query, terms=True, limit=None)
            results.fragmenter = highlight.SentenceFragmenter(charlimit = None)

            for i in range(len(results)):
                with open(results[i]["path"],'r',encoding='utf-8') as fileobj:
                    filecontents = fileobj.read()
                list_result51.append([results[i]['title'],(results[i].highlights("content", text=filecontents )).replace('...','<br>') ])
    return list_result51      



@app.route("/", methods=['GET','POST'])
@app.route("/index", methods=['GET','POST'])
def index():
    if request.method == 'POST':
        try:

            if 'search_51' in request.form:
                search_51 = request.form["search_51"]
                list_result51 = search_entered_query(search_51)
                #print(list_result51)
                count_rows51 = f"The count of documents matched is {len(list_result51)}."
                t_headings51 = ["Document Name", "Matched Lines"]

                return render_template('index.html',count_rows51=count_rows51,t_headings51=t_headings51,list_result51=list_result51)

            if 'another':
                pass

        except Exception as e:
            print(e,"Error has occured")
            

    return render_template('index.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)

