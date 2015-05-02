import requests
import re, os
from sqlite3 import dbapi2 as sqlite
import sqlite3
import csv
import random
import threading
import Queue

""" 
Scrapes catchment areas from primarycare.nhs.uk. Is this evil?
"""

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
    """ Get a page by organization code. Working example: ?oc=A85007&h=800&w=1000&if=0 """
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

class scraper(threading.Thread):
    def __init__(self, ocs, queue):
        """ A threaded scraper """
        threading.Thread.__init__(self)
        self.ocs = ocs
        self.queue = queue

    def run(self):
        """ Scrape all ocs assigned to this thread """
        print "Started thread {:}".format(threading.current_thread().name)
        for index, oc in enumerate(self.ocs):
            name, polygon = get_page(oc)
            self.queue.put((oc, name, polygon))


if __name__ == '__main__':
    # Get a list of ids yet to scrape
    db_connection = sqlite.connect("practices.db")
    db_curs = db_connection.cursor()
    db_curs.execute("SELECT id FROM practices")
    done = set([x[0] for x in db_curs.fetchall()])
    todo = list(get_ocs() - done)
    main_queue = Queue.Queue()

    # Divide them into batches
    MAX_THREADS = 10
    BATCH_SIZE = len(todo)/MAX_THREADS
    print "max threads: {:}\nbatch size: {:}".format(MAX_THREADS, BATCH_SIZE)
    batches = [todo[i:i+BATCH_SIZE] for i in range(0, len(todo), BATCH_SIZE)]

    scrapers = [scraper(batch, main_queue) for batch in batches]
    for s in scrapers:
        s.start()

    while True:
        oc, name, polygon = main_queue.get(True, 5)
        if not name: 
            db_curs.execute("INSERT INTO practices (id, broken)\
                             VALUES ('{0}', '{1}')".format(oc, 1))
        else:
            db_curs.execute("INSERT INTO practices (id, name, polygon, broken)\
                             VALUES ('{0}', '{1}', '{2}', '{3}')".format(oc, name, str(polygon), 0))
        if random.random()<0.1: db_connection.commit()

