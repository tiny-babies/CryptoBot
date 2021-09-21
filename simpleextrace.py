import datetime
import time
from bot import cbpGetHistoricRates, addMovingAverages, addMomentumIndicators, saveHistoricRates, addHistoricRatesToFile

def extract():
    day = 24
    year = day * 365

    length_of_record = year*2
    iso_start = datetime.datetime.now() - datetime.timedelta(hours=length_of_record)
    iso_end = iso_start + datetime.timedelta(hours=300)
    data = cbpGetHistoricRates(
        'BTC-GBP', 3600, iso_start.isoformat(), iso_end.isoformat())
    # data = addMovingAverages(data)
    # data = addMomentumIndicators(data)
    # saveHistoricRates('cbpGetHistoricRates.csv', data)
    # print(data)
    # length_of_record -= 300
    # extractHelper(length_of_record)

def extractHelper(rec_len):
    while rec_len > 0:
        time.sleep(5)
        iso_start = datetime.datetime.now() - datetime.timedelta(hours=rec_len)
        if (rec_len - 300) <= 0:
            iso_end = datetime.datetime.now()
            data = cbpGetHistoricRates(
                'BTC-GBP', 3600, iso_start.isoformat(), iso_end.isoformat())
        else:
            iso_end = iso_start + datetime.timedelta(hours=300)
            data = cbpGetHistoricRates('BTC-GBP', 3600, iso_start.isoformat(), iso_end.isoformat())
        data = addMovingAverages(data)
        data = addMomentumIndicators(data)
        addHistoricRatesToFile('cbpGetHistoricRates.csv', data)
        print(data)
        rec_len -= 300
        
extract()



#https://api.pro.coinbase.com/products/BTC-GBP/candles?granularity=3600&start=2019-09-19T15:00:35.467615&end=2019-10-02T03:00:35.467615
