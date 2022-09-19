from datetime import datetime as dt
from datetime import timedelta
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

def scrape(channel, year, date, end):
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

        with open(date_str, 'wb') as fp:
            fp.write(data)

        date += timedelta(days=1)

def define_file(year, channel, symbol=None):
    file = "{}_{}.csv".format(year, channel)
    if (symbol!=None):
        file = "{}_{}_{}.csv".format(year, channel, symbol)
    return file

def merge(year, channel):
    print("Generating CSV for {}".format(year))
    files = sorted(glob.glob("{}*".format(year)))
    first = True
    file = define_file(year, channel)
    with open(file, 'wb') as out:
        for f in files:
            with open(f, 'rb') as fp:
                if first is False:
                    fp.readline()
                first = False
                shutil.copyfileobj(fp, out)
    for f in files:
        os.unlink(f)

def filter(year, channel, symbol):
    if (symbol==None):
        return
    print("Filtering CSV for {}".format(year), "symbol: {}".format(symbol))
    file = define_file(year, channel)
    file_filtered = define_file(year, channel, symbol)
    df_csv = pd.read_csv(file)
    df_csv = df_csv.loc[df_csv['symbol'] == symbol]
    df_csv.to_csv(path_or_buf=file_filtered, index=False)
    move(channel, file)
    move(channel, file_filtered)

def move(channel, file):
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
    args = parser.parse_args()

    start = dt.strptime(args.start, '%Y%m%d')
    end = dt.strptime(args.end, '%Y%m%d') if args.end else dt.utcnow()
    channel = args.channel
    symbol = args.symbol

    years = list(range(start.year, end.year + 1))

    starts = [dt(year, 1, 1) for year in years]
    starts[0] = start

    for year, start in zip(years, starts):
        scrape(channel, year, start, end)
        merge(year, channel)

    for year, start in zip(years, starts):
        filter(year, channel, symbol)