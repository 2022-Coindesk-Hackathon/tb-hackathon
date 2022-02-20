from boto3 import resource
from dataclasses import dataclass
from decimal import Decimal
from dotenv import load_dotenv
from os import getenv
from s3_helper import CSVStream
from typing import Any
import datetime
import math
# import scipy
# from scipy.optimize import curve_fit
load_dotenv()

BUY = "buy"
SELL = "sell"

BUCKET = getenv("BUCKET_NAME")

XBT_2018_KEY = "xbt.usd.2018"
XBT_2020_KEY = "xbt.usd.2020"

ETH_2018_KEY = "eth.usd.2018"
ETH_2020_KEY = "eth.usd.2020"

S3 = resource("s3")
SELECT_ALL_QUERY = 'SELECT * FROM S3Object'

# Example s3 SELECT Query to Filter Data Stream
#
# The where clause fields refer to the timestamp column in the csv row.
# To filter the month of February, for example, (start: 1517443200, end: 1519862400) 2018
#                                               (Feb-01 00:00:00  , Mar-01 00:00:00) 2018
#
# QUERY = '''\
#     SELECT *
#     FROM S3Object s
#     WHERE CAST(s._4 AS DECIMAL) >= 1514764800
#       AND CAST(s._4 AS DECIMAL) < 1514764802
# '''
QUERY = '''\
    SELECT *
    FROM S3Object s
    WHERE CAST(s._4 AS DECIMAL) >= 1514764800
      AND CAST(s._4 AS DECIMAL) < 1514764820
'''
STREAM = CSVStream(
    'select',
    S3.meta.client,
    key=XBT_2018_KEY,
    bucket=BUCKET,
    expression=SELECT_ALL_QUERY,
)

@dataclass
class Trade:
    trade_type: str # BUY | SELL
    base: str
    volume: Decimal


def percDiff(timestamp,value,coin):

    #input: timestamp:int, value:int
    #return the percent difference between the actual value and our predicted value
    #if actual value is lower then predicted value return negative value
    pred = generalpred(timestamp,coin)
    intvalue=float(value)
    pred_error=(abs(pred-intvalue)/((pred+intvalue)/2))*100
    if intvalue<pred:
        pred_error*=-1
    # print(f"value:{value}  expeceted:{pred}")
    return pred_error

def generalpred(timestamp,coin):
    #input: timestamp:int
    #return: expected value: int
    if coin.lower()=="eth":
        inttimestamp=float(timestamp)
        # print(datetime.datetime.fromtimestamp(inttimestamp))
        inttimestamp-=1514764799
        return (.999964067*(math.log(inttimestamp)+1038.9))   #function for predicting data in for a*log(x)+b=y
    if coin.lower()=="xbt":
        inttimestamp=float(timestamp)
        # print(timestamp)
        inttimestamp-=1514764799
        return (.999747701*(math.log(inttimestamp)+11604.0562))

def algorithm(csv_row: str, context: dict[str, Any],):
    #values in dic[timestamp] -> (up/down,percvalue)
    if "\n" in csv_row:
        response=yield None
    split=csv_row.split(",")
    
    if len(split)!=4:
        response=yield None
    time=float(split[3])
    value=float(split[1])
    if len(split[3])>=10:
        coin=split[0][5:8]
        if coin.lower()=="eth":
            difference=percDiff(split[3],split[1],"eth")
        elif coin.lower()=="xbt":
            percDiff(split[3],split[1],"eth")
        else:
            yield None

        i=0
        while context.get(str(time-i))==None or time-i>1514764799:
            i-=1
        if abs(context.get(str(time-i))[1])>=abs(difference):
            #closer to log
            if difference>0:
                #price is going down and closer to log
                context[str(time)]=("down",difference)
            else:
                context[str(time)]=("up",difference)
                #price is going up and closer to log

        else:
            #further from log
            if difference>0:
                #price is going up and further from log
                context[str(time)]=("up",difference)
            else:
                #price is going down and further from log
                context[str(time)]=("down",difference)
        
        past=[0,0,0]
        #up down None
        depth=100000
        for i in range(depth):
            data=context.get(str(time-i))
            if data==None:
                past[2]+=1
            if data[0]==up:
                past[0]+=1
            if data[1]==down:
                past[1]+=1
        avgdata=[]
        avgdata.append(past[0]/depth)
        avgdata.append(past[1]/depth)
        avgdata.append(past[2]/depth)
        current=context.get(str(time))
        if current[0]=="up" and avgdata[1]>=.7:
            x=context.get(buys)
            invested=x[0][1]+x[1][1]
            capital=1000000
            if coin.lower()=="xbt":
                context[buys]=[("xbt",x[0][1]+(capital-invested)*.05),("eth",x[1][1])]
            else: context[buys]=[("xbt",x[0][1]),("eth",x[1][1]+(capital-invested)*.05)]
            volume=((capital-invested)*.05)/value
            response=yield Trade("BUY",coin.lower(),Decimal(volume))
            #buy
            # pass
        if current[0]=="down" and avgdata[0]>=.7:
            x=context.get(buys)
            if x!= None:
                if coin.lower()=="xbt":
                    volume=(x[0][1]*.10)/value
                    response=yield Trade("SELL",coin.lower(),Decimal(volume))
                else:
                    volume=(x[0][1]*.10)/value
                    response=yield Trade("SELL",coin.lower(),Decimal(volume))
            #sell
            # pass

        #   (1) okfq-xbt-usd,14682.26,2,1514765115
    else:     
        response = yield None
    """ Trading Algorithm

    Add your logic to this function. This function will simulate a streaming
    interface with exchange trade data. This function will be called for each
    data row received from the stream.

    The context object will persist between iterations of your algorithm.

    Args:
        csv_row (str): one exchange trade (format: "exchange pair", "price", "volume", "timestamp")
        context (dict[str, Any]): a context that will survive each iteration of the algorithm

    Generator:
        response (dict): "Fill"-type object with information for the current and unfilled trades
    
    Yield (None | Trade | [Trade]): a trade order/s; None indicates no trade action
    """
    # algorithm logic...

     # example: Trade(BUY, 'xbt', Decimal(1))

    # algorithm clean-up/error handling...



def func(a,x,b):
    return (a*(math.log(x)))+b

if __name__ == '__main__':
    for count,row in enumerate(STREAM.iter_records()):
        print(algorithm(row,{}))

    #     if "\n" in row:
    #         continue
    #     split=row.split(",")
    #     if len(split)!=4:
    #         continue

    #     if split[0][5:8]=="eth":
    #         if (float(split[1])<=(prior*2) and float(split[1])>(prior/2)):
    #             if len(split[3])>=10:
    #                 xdata.append(float(split[3])-1514764800)
    #                 ydata.append(float(split[1]))

    # x,y=scipy.optimize.curve_fit(func,xdata,ydata)
    # print(x)

    # print(datetime.datetime.fromtimestamp(1514765799))

# Example Interaction
#
# Given the following incoming trades, each line represents one csv row:
#   (1) okfq-xbt-usd,14682.26,2,1514765115
#   (2) okf1-xbt-usd,13793.65,2,1514765115
#   (3) stmp-xbt-usd,13789.01,0.00152381,1514765115
#
# When you receive trade 1 through to your algorithm, if you decide to make
# a BUY trade for 3 xbt, the order will start to fill in the following steps
#   [1] 1 unit xbt from trade 1 (%50 available volume from the trade data)
#   [2] 1 unit xbt from trade 2
#   [3] receiving trade 3, you decide to put in another BUY trade:
#       i. Trade will be rejected, because we have not finished filling your 
#          previous trade
#       ii. The fill object will contain additional fields with error data
#           a. "error_code", which will be "rejected"; and
#           b. "error_msg", description why the trade was rejected.
#
# Responses during these iterations:
#   [1] success resulting in:
#       {
#           "price": 14682.26,
#           "volume": 1,
#           "unfilled": {"xbt": 2, "eth": 0 }
#       }
#   [2]
#       {
#           "price": 13793.65,
#           "volume": 1,
#           "unfilled": {"xbt": 1, "eth": 0 }
#       }
#   [3]
#       {
#           "price": 13789.01,
#           "volume": 0.000761905,
#           "error_code": "rejected",
#           "error_msg": "filling trade in progress",
#           "unfilled": {"xbt": 0.999238095, "eth": 0 }
#       }
#
# In step 3, the new trade order that you submitted is rejected; however,
# we will continue to fill that order that was already in progress, so
# the price and volume are CONFIRMED in that payload.
