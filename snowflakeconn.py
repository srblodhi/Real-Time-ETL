import pandas as pd
import os
import snowflake.connector
from collections.abc import MutableMapping
from consumer import kafka_Consumer

# create snowflake connector
ctx = snowflake.connector.connect(
    user='srblodhi',
    password='$RBcool2802',
    account='gflwsoo-ejb21629',
    role='ACCOUNTADMIN',
    database='STOCK_MARKET_PRICES',
    schema ='PUBLIC'
    )

cs = ctx.cursor()

# SQL query to create table in snowflake
sql = """ CREATE TABLE stock_prices(
    ID int not null primary key identity(1,1),
    "goog.maxAge" int,
    "goog.priceHint" int,
    "goog.previousClose" float,
    "goog.open" float,
    "goog.dayLow" float,
    "goog.dayHigh" float,
    "goog.regularMarketPreviousClose" float,
    "goog.regularMarketOpen" float,
    "goog.regularMarketDayLow" float,
    "goog.regularMarketDayHigh" float,
    "goog.payoutRatio" float,
    "goog.beta" float,
    "goog.trailingPE" float, 
    "goog.forwardPE" float,
    "goog.volume" int,
    "goog.regularMarketVolume" int, 
    "goog.averageVolume" int,
    "goog.averageVolume10days" int,
    "goog.averageDailyVolume10Day" int,
    "goog.bid" float,
    "goog.ask" float,
    "goog.bidSize" int,
    "goog.askSize" int,
    "goog.marketCap" int,
    "goog.fiftyTwoWeekLow" float,
    "goog.fiftyTwoWeekHigh" float, 
    "goog.priceToSalesTrailing12Months" float,
    "goog.fiftyDayAverage" float,
    "goog.twoHundredDayAverage" float,
    "goog.trailingAnnualDividendRate" float,
    "goog.trailingAnnualDividendYield" float,
    "goog.currency" VARCHAR(5),
    "goog.fromCurrency" VARCHAR(5),
    "goog.toCurrency" VARCHAR(5),
    "goog.lastMarket" VARCHAR(50),
    "goog.coinMarketCapLink" VARCHAR(50),
    "goog.algorithm" VARCHAR(50),
    "goog.tradeable" boolean
);
"""

# execute the query using snowflake executer
cs.execute(sql)

# fetch the output of the snowflake console
first_row = cs.fetchone()
print(first_row[0])