#!/usr/bin/python
import sqlite3
from flask import Flask, jsonify, request
from flask import g
from flask import render_template

app = Flask(__name__)
app.config.from_envvar("NHS_SETTINGS")

def connect_db():
    return sqlite3.connect(app.config["DATABASE"])

@app.before_request
def before_request():
    g.db = connect_db()

@app.teardown_request
def teardown_request(exception):
    db = getattr(g, "db", None)
    if db is not None:
        db.close()

@app.route("/")
def mainpage():
    return render_template('index.html', name="eggboi")

@app.route("/polygon/<oc>", methods=["GET"])
def polygon(oc):
    cur = g.db.execute("select name, polygon from practices where id='{:}'".format(oc))
    name, polygon = cur.fetchone()
    polygon = eval(polygon) # TODO: OUCH
    return jsonify(oc = oc, name = name, points = polygon)

if __name__ == "__main__":
    app.debug = True
    app.run()
