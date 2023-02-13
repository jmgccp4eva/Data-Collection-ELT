import time
import sqlite3
import json
import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
import concurrent.futures as futures

import batch_upload
import config
import data_collection
import urllib
import urllib3
import sqliteDBProcessing
from time import perf_counter as perf

def main():
    # BUILD SQLITE DATABASE
    start = perf()
    sqliteDBProcessing.show_list_of_tables("6degreesRev1.db")

    # data_collection.get_original_data("6degreesRev1.db")
    # data_collection.build_us_series_only("6degreesRev1.db","data.db")

    batch_upload.create_data_db('6degreesRev1.db','data.db')

    # actor_values = {"m":"i","it":"a","ti":4587,"ii":"%20","who":"Jason%20Grebe","pob":"Cape%20Girardeau,%20Missouri%20USA","b":"1977-07-02","d":"None"}

    end = perf()
    print(f"{end-start}")

if __name__=='__main__':
    main()