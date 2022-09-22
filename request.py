from datetime import datetime as dt
from datetime import timedelta
import time
import requests
from limiter import get_limiter

def endpoint_bitmex_api(channel: str, symbol: str):
    endpoint = 'https://www.bitmex.com/api/v1/{}?symbol={}&reverse=false'.format(channel, symbol)
    endpoint = endpoint+'&startTime={}&endTime={}'
# https://www.bitmex.com/api/v1/funding?symbol=XBTUSD&reverse=false&startTime=2021-01-20T17%3A16%3A29.029Z&endTime=2021-09-20T17%3A16%3A29.029Z
    return endpoint

def endpoint_bitmex_aws(channel: str):
    # https://public.bitmex.com/?prefix=data/trade/
    endpoint = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/{}/'
    endpoint = endpoint.format(channel)
    endpoint = endpoint+'{}.csv.gz'
    return endpoint

def define_endpoint(channel: str, symbol: str):
    if channel == "funding":
        endpoint = endpoint_bitmex_api(channel, symbol)
    else:
        endpoint = endpoint_bitmex_aws(channel)
    return endpoint

def define_request(date: dt, end_date:dt, channel: str, symbol: str):
    endpoint = define_endpoint(channel, symbol)
    if channel == "funding":
        request = endpoint.format(date, end_date)
    else:
        date_str = date.strftime('%Y%m%d')
        request = endpoint.format(date_str)
    return request

def make_request(date: dt, channel: str, symbol: str, days_interval: int):
    endpoint = define_endpoint(channel, symbol)
    if channel == "funding":
        startTime = date
        endTime = startTime + timedelta(days=days_interval)
        request = endpoint.format(startTime, endTime)
        while 1:
            if get_limiter():
                print(get_limiter())
                r = requests.get(request)
                return r
            else: 
                SLEEP_SECONDS = 1
                print("Sleep for {} seconds".format(SLEEP_SECONDS))
                time.sleep(SLEEP_SECONDS)
    else:
        date_str = date.strftime('%Y%m%d')
        request = endpoint.format(date_str)
        r = requests.get(request)
        return r

