# -*- coding: utf-8 -*-
"""
Created on Fri Mar 31 10:23:43 2023

@author: EspenNordsveen
"""
import datetime

from datetime import timedelta

import pandas as pd


data = {"el": ["2s=P10_AAW01_BJ01:M_P"], "spot": ["spotprice_el_Skive"]}


def handle(client, data):
    today = datetime.date.today()
    t = datetime.time(hour=23, minute=59, second=59)

    time_yesterday = datetime.datetime.combine(today, t) - datetime.timedelta(hours=24)

    dps_electrical = client.datapoints.retrieve(
        start="2d-ago", end="now", external_id=data["el"], aggregates="average", granularity="1h"
    ).to_pandas()
    dps_spot = client.datapoints.retrieve(
        start="2d-ago", end="now", external_id=data["spot"], aggregates="average", granularity="1h"
    ).to_pandas()
    # Find yesterdays electraical consumption
    yesterday = datetime.datetime.today() - timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")
    yesterday_consumption = dps_electrical[dps_electrical.index.date == pd.to_datetime(yesterday).date()].sum()
    client.datapoints.insert([(time_yesterday, yesterday_consumption[0])], external_id="daily_electrical_consumption")

    yesterday_hourly_consumption = dps_electrical[dps_electrical.index.date == pd.to_datetime(yesterday).date()]
    yesterday_spot = dps_spot[dps_spot.index.date == pd.to_datetime(yesterday).date()]
    yesterday_cost = []
    for i in range(len(yesterday_spot)):
        yesterday_cost.append(yesterday_hourly_consumption.iloc[i].values[0] * yesterday_spot.iloc[i].values[0] / 1000)

    client.datapoints.insert([(time_yesterday, sum(yesterday_cost))], external_id="daily_electrical_cost")
