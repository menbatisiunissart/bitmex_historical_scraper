from datetime import datetime as dt
from datetime import timedelta
from io import StringIO
import argparse
import gzip
import glob
import os
import shutil
import time
from typing import Any
import pandas as pd
from request import make_request

def define_days_interval(channel: str):
    if channel == "funding":
        days_interval = 30
    else:
        days_interval = 1
    return days_interval

def read_data(date_str: str, channel: str, content: Any):
    if channel == "funding":
        data = content
    else:
        with open(date_str, 'wb') as fp:
            fp.write(content)

        with gzip.open(date_str, 'rb') as fp:
            data = fp.read()
    return data

def scrape(year: int, date: dt, end: dt, channel: str, symbol: str):
    
    end_date = min(dt(year, 12, 31), dt.today() - timedelta(days=1))
    while date <= end_date and date <= end:
        days_interval = define_days_interval(channel)
        print("Processing {}...".format(date))
        count = 0
        while True:
            r = make_request(date, channel, symbol, days_interval)
            if r and r.status_code == 200:
                break
            else:
                count += 1
                if count == 10:
                    r.raise_for_status()
                print("Error processing {} - {}, trying again".format(date, r.status_code))
                time.sleep(10)

        date_str = date.strftime('%Y%m%d')
        data = read_data(date_str, channel, r.content)
        file = define_file(date_str, channel, symbol)
        filter(data, file, date_str, symbol)
        move(file, channel, period='day', year=year)

        date += timedelta(days=days_interval)

def define_file(date, channel, symbol=None):
    file = "{}_{}.csv".format(date, channel)
    if (symbol!=None):
        file = "{}_{}_{}.csv".format(date, channel, symbol)
    return file

def file_list(channel, period, year):
    files = sorted(glob.glob("./{}/{}/{}/*".format(channel, period, year)))
    return files

def clean(year):
    files = sorted(glob.glob("{}*".format(year)))
    for f in files:
        os.unlink(f)

def merge_file(year, channel):
    print("Generating CSV for year {} channel {}".format(year, channel))
    files = file_list(channel, period='day', year=year)
    file = define_file(year, channel)
    # joining files with concat and read_csv
    df = pd.concat(map(pd.read_csv, files), ignore_index=True)
    df.to_csv(path_or_buf=file, index=False)
    move(file, channel, period='year', year=year)

def filter(bytes_data, file, date, symbol):
    s=str(bytes_data,'utf-8')
    data = StringIO(s)
    df_csv = pd.read_csv(data)
    if (symbol!=None and  not df_csv.empty):
        print("Filtering data for {}".format(date), "symbol: {}".format(symbol))
        df_csv = df_csv.loc[df_csv['symbol'] == symbol]
    df_csv.to_csv(path_or_buf=file, index=False)

def move(file, channel, period, year):
    # Create the directory first if it doesn't exist:
    path_channel = os.path.join('./', channel)
    if not os.path.exists(path_channel):
        os.mkdir(path_channel)
    path_period = os.path.join(path_channel, period)
    if not os.path.exists(path_period):
        os.mkdir(path_period)
    year=str(year)
    path_year = os.path.join(path_period, year)
    if not os.path.exists(path_year):
        os.mkdir(path_year)
    path_file = os.path.join(path_year, file)
    shutil.move(file, path_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='BitMex historical data scraper. Scrapes files into single year CSVs')
    parser.add_argument('--start', default="20141122", help='start date, in YYYYMMDD format. Default is 2014-11-22, the earliest data date for BitMex')
    parser.add_argument('--end', default=None, help='end date, in YYYYMMDD format. Default is yesterday')
    parser.add_argument('--channel', default="trade", help='BitMex historical data channel. Support "trade" and "quote" channel')
    parser.add_argument('--symbol', default=None, help='symbol filter to apply. Default is None, no filtering will be applied')
    parser.add_argument('--merge', default=True, help='Merge monthly data into yearly data')
    args = parser.parse_args()

    start = dt.strptime(args.start, '%Y%m%d')
    end = dt.strptime(args.end, '%Y%m%d') if args.end else dt.utcnow()
    channel = args.channel
    symbol = args.symbol
    merge = args.merge

    years = list(range(start.year, end.year + 1))

    starts = [dt(year, 1, 1) for year in years]
    starts[0] = start

    for year, start in zip(years, starts):
        scrape(year, start, end, channel, symbol)
        if (merge==True):
            merge_file(year, channel)
        clean(year)
        