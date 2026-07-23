EODHD

Version 1

Code is to read data from email stock tips and store in a sqlite db.

For each tip, data from eodhd from before and after the tip date is also to be stored.
Daily and five-minutely data.

Goal: deploy script to run daily in production which fetches tips and price data.

Version 2

Backtest basic buy/hold/sell decision making approaches.

Calculate Kelly's optimal f.