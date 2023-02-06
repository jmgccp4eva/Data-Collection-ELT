import time
import sqlite3
import concurrent.futures as futures
import config
import data_collection
import sqliteDBProcessing
from time import perf_counter as perf

def main():
    # BUILD SQLITE DATABASE
    start = perf()
    sqliteDBProcessing.show_list_of_tables("6degreesRev1.db")

    # data_collection.get_original_data("6degreesRev1.db")
    data_collection.build_us_series_only("6degreesRev1.db","data.db")
    end = perf()
    print(f"{end-start}")

if __name__=='__main__':
    main()