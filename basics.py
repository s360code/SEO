# -*- coding: utf-8 -*-
"""Basics.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/16Eo5sRywfr6XUEQgRM0bMNybVICCdrP5
"""

#imports
import requests
from json import dumps
import pandas as pd
import datetime
from google.colab import output
import gspread
from google.colab import auth, output
from oauth2client.client import GoogleCredentials
from google.auth import default
from datetime import datetime 
from requests.auth import HTTPBasicAuth
import json
from dateutil.relativedelta import relativedelta
import time
import re
from gspread_dataframe import get_as_dataframe, set_with_dataframe

class Basics:
    def __init__(self, sheet_id, sheet, startrow):
        auth.authenticate_user()
        self.creds, _ = default()
        self.gc = gspread.authorize(self.creds)
        self.sheet_id = sheet_id
        self.connection = self.gc.open_by_url("https://docs.google.com/spreadsheets/d/"+str(self.sheet_id))
        self.sheet = self.connection.worksheet(sheet)
        self.startrow = startrow

    def data_from_sheet(self):
        dataframe = get_as_dataframe(self.sheet)
        dataframe.columns = dataframe.iloc[self.startrow, :].tolist()
        dataframe = dataframe.drop(range(self.startrow+1))
        return dataframe
