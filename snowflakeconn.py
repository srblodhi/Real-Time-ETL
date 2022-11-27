import pandas as pd
import os
import snowflake.connector


ctx = snowflake.connector.connect(
    user='srblodhi',
    password='$RBcool2802',
    account='gflwsoo-ejb21629',
    role='ACCOUNTADMIN',
    database='STOCK_MARKET_PRICES',
    schema ='PUBLIC'
    )

cs = ctx.cursor()

sql = "select 1"
cs.execute(sql)

first_row = cs.fetchone()
print(first_row[0])