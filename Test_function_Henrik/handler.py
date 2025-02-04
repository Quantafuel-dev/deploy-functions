# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 12:52:49 2025

@author: Henrik.Rost.Breivik
"""


def handle(data, client):
    print("I got the following data:")
    print(data)

    if not ("a" in data and "b" in data):
        raise KeyError("Data should contain both keys: 'a' and 'b'")

    print("Hello world")
    print("Will now return updated data")

    return data
