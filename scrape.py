from datetime import datetime as dt
from datetime import timedelta
from io import StringIO
import argparse
import gzip
import glob
import os
import shutil
import time

import requests
import pandas as pd

def define_endpoint(channel):
    # https://public.bitmex.com/?prefix=data/trade/
    endpoint = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/{}/'
    endpoint = endpoint.format(channel)
    endpoint = endpoint+'{}.csv.gz'
    return endpoint

def scrape(year, date, end, channel, symbol):
    endpoint = define_endpoint(channel)
    end_date = min(dt(year, 12, 31), dt.today() - timedelta(days=1))

    while date <= end_date and date <= end:
        date_str = date.strftime('%Y%m%d')
        print("Processing {}...".format(date))
        count = 0
        while True:
            r = requests.get(endpoint.format(date_str))
            if r.status_code == 200:
                break
            else:
                count += 1
                if count == 10:
                    r.raise_for_status()
                print("Error processing {} - {}, trying again".format(date, r.status_code))
                time.sleep(10)

        with open(date_str, 'wb') as fp:
            fp.write(r.content)

        with gzip.open(date_str, 'rb') as fp:
            data = fp.read()

        file = define_file(date_str, channel, symbol)
        filter(data, file, date_str, symbol)
        move(file, channel)

        date += timedelta(days=1)

def define_file(date, channel, symbol=None):
    file = "{}_{}.csv".format(date, channel)
    if (symbol!=None):
        file = "{}_{}_{}.csv".format(date, channel, symbol)
    return file

def temp_file_list(year):
    files = sorted(glob.glob("{}*".format(year)))
    return files

def clean(year):
    files = sorted(glob.glob("{}*".format(year)))
    for f in files:
        os.unlink(f)

def merge_file(year, channel):
    print("Generating CSV for {}".format(year))
    files = temp_file_list(year)
    first = True
    file = define_file(year, channel)
    with open(file, 'wb') as out:
        for f in files:
            with open(f, 'rb') as fp:
                if first is False:
                    fp.readline()
                first = False
                shutil.copyfileobj(fp, out)

def filter(bytes_data, file, date, symbol):
    s=str(bytes_data,'utf-8')
    data = StringIO(s)
    df_csv = pd.read_csv(data)
    if (symbol!=None):
        print("Filtering data for {}".format(date), "symbol: {}".format(symbol))
        df_csv = df_csv.loc[df_csv['symbol'] == symbol]
    df_csv.to_csv(path_or_buf=file, index=False)

def move(file, channel):
    # Create the directory first if it doesn't exist:
    path = os.path.join('./', channel)
    if not os.path.exists(path):
        os.mkdir(path)
    new_file_path = os.path.join(path, file)
    shutil.move(file, new_file_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='BitMex historical data scraper. Scrapes files into single year CSVs')
    parser.add_argument('--start', default="20141122", help='start date, in YYYYMMDD format. Default is 2014-11-22, the earliest data date for BitMex')
    parser.add_argument('--end', default=None, help='end date, in YYYYMMDD format. Default is yesterday')
    parser.add_argument('--channel', default="trade", help='BitMex historical data channel. Support "trade" and "quote" channel')
    parser.add_argument('--symbol', default=None, help='symbol filter to apply. Default is None, no filtering will be applied')
    parser.add_argument('--merge', default=False, help='Merge monthly data into yearly data')
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
        