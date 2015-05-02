import requests
import re, os
from sqlite3 import dbapi2 as sqlite
import sqlite3
import csv
import random
import threading

# Regexes
CC0FINDER = re.compile(r"^\s*var\s*CC0\s*\=\s*\[new\sgoogle\.maps.*$", re.M)
LATLNGEXTRACTOR = re.compile(r"-?\d+\.\d+\,\s?-?\d+\.\d+", re.M)
NAMEEXTRACTOR = re.compile(r"maps.Animation.DROP.*title:\'(.*)\'", re.M)

def get_polygon(html):
    """ Extracts a polygon from some HTML """
    points = CC0FINDER.findall(html)
    if points:
        points = LATLNGEXTRACTOR.findall(points[0])
        points = [(float(a), float(b)) for a,b in [x.split(",") for x in points]]
        return points
    else:
        return None

def get_name(html):
    """ Extracts a practice name from some HTML """
    matches = NAMEEXTRACTOR.search(html)
    return matches.groups()[0] if matches else None

def get_page(oc):
    """ Get a page by the mystery key. Working example: ?oc=A85007&h=800&w=1000&if=0 """
    url = "https://www.primarycare.nhs.uk/publicfn/catchment.aspx"
    payload = {"oc":oc, "h":1, "w":1, "if":0}
    r = requests.get(url, params = payload)
    name = get_name(r.text)
    polygon = get_polygon(r.text)
    print "{0}, {1}, {2}".format(oc, r.status_code, name if name else "failed")
    return name, polygon

def get_ocs(filename="data/epraccur.csv"):
    """ Get all organization codes """
    f = open(filename)
    ocs = {str(x[0]) for x in csv.reader(f)}
    f.close()
    return ocs

if __name__ == '__main__':
    db_connection = sqlite.connect("practices.db")
    db_curs = db_connection.cursor()

    db_curs.execute("SELECT id FROM practices")
    done = set([x[0] for x in db_curs.fetchall()])
    todo = get_ocs() - done

    for index, oc in enumerate(todo):
        name, polygon = get_page(oc)
        if not name: 
            db_curs.execute("INSERT INTO practices (id, broken)\
                             VALUES ('{0}', '{1}')".format(oc, 1))
        else:
            db_curs.execute("INSERT INTO practices (id, name, polygon, broken)\
                             VALUES ('{0}', '{1}', '{2}', '{3}')".format(oc, name, str(polygon), 0))
        if random.random()<0.1: db_connection.commit()

