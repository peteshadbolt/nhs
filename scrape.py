#!/usr/bin/python
import re
import os
import csv
import random
import threading
import Queue
import requests
from sqlite3 import dbapi2 as sqlite
import time

"""
Scrapes catchment areas from primarycare.nhs.uk. Is this evil?
"""

# Regexes
DATABASE = "practices.db"
MAX_THREADS = 1
THROTTLE = 1
ROOT_URL = "https://www.primarycare.nhs.uk/publicfn/catchment.aspx"
OCLISTFILE = "data/epraccur.csv"
OCFINDER = re.compile(r"^\s*var\s*CC0\s*\=\s*\[new\sgoogle\.maps.*$", re.M)
LATLNGEXTRACTOR = re.compile(r"-?\d+\.\d+\,\s?-?\d+\.\d+", re.M)
NAMEEXTRACTOR = re.compile(r"maps.Animation.DROP.*title:\'(.*)\'", re.M)


def get_polygon(html):
    """ Extracts a polygon from some HTML """
    points = OCFINDER.findall(html)
    if points:
        points = LATLNGEXTRACTOR.findall(points[0])
        points = [(float(a), float(b))
                  for a, b in [x.split(",") for x in points]]
        return points
    else:
        return None


def get_name(html):
    """ Extracts a practice name from some HTML """
    matches = NAMEEXTRACTOR.search(html)
    return matches.groups()[0] if matches else None


def get_page(oc):
    """ Get a page by organization code. Working example: ?oc=A85007&h=800&w=1000&if=0 """
    url = ROOT_URL
    payload = {"oc": oc, "h": 1, "w": 1, "if": 0}
    r = requests.get(url, params=payload)
    name = get_name(r.text)
    polygon = get_polygon(r.text)
    print "{:} {:}".format(oc, name if name else "failed")
    return name, polygon


class scraper(threading.Thread):

    def __init__(self, ocs, queue):
        """ A threaded scraper """
        threading.Thread.__init__(self)
        self.ocs = ocs
        self.daemon = True
        self.queue = queue

    def run(self):
        """ Scrape all ocs assigned to this thread """
        for index, oc in enumerate(self.ocs):
            name, polygon = get_page(oc)
            self.queue.put((oc, name, polygon))
            time.sleep(THROTTLE)


if __name__ == '__main__':

    # Load the list of organization codes
    all_ocs = {str(x[0]) for x in csv.reader(open(OCLISTFILE))}

    # Examine current progress
    db_connection = sqlite.connect(DATABASE)
    db_curs = db_connection.cursor()
    db_curs.execute("SELECT count(*) FROM practices")
    count_all = db_curs.fetchone()[0]
    db_curs.execute("SELECT count(*) FROM practices where broken=0")
    count_working = db_curs.fetchone()[0]
    print "Currently have good data on {:} practices ({:} total)".format(count_working, count_all)
    print "There are {:} organization codes listed in the CSV file. You're about {:}% done.".format(len(all_ocs), 100 * count_all / len(all_ocs))
    raw_input("Hit enter to start scraping.")

    # Generate the TODO list
    db_curs.execute("SELECT id FROM practices")
    done = set([x[0] for x in db_curs.fetchall()])
    todo = list(all_ocs - done)
    main_queue = Queue.Queue()

    # Divide them into batches, start the threads
    BATCH_SIZE = len(todo) / MAX_THREADS
    batches = [todo[i:i + BATCH_SIZE] for i in range(0, len(todo), BATCH_SIZE)]
    scrapers = [scraper(batch, main_queue) for batch in batches]
    for s in scrapers:
        s.start()

    # Collect messages and dump to the database
    while True:
        oc, name, polygon = main_queue.get(True, 5)
        if not name:
            db_curs.execute("INSERT INTO practices (id, broken)\
                             VALUES ('{0}', '{1}')".format(oc, 1))
        else:
            db_curs.execute("INSERT INTO practices (id, name, polygon, broken)\
                             VALUES ('{0}', '{1}', '{2}', '{3}')".format(oc, name, str(polygon), 0))
        if random.random() < 0.1:
            db_connection.commit()

    # Be polite
    db_connection.commit()
    db_connection.close()
