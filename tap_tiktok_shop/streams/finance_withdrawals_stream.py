import requests
import singer_sdk
from singer_sdk import Stream
from singer_sdk import typing as th
from typing import Optional, Iterable, Dict
import json, time
from tap_tiktok_shop.generate_signature import generate_signature
import urllib.parse
from urllib.parse import urlencode, urlparse, parse_qs
import requests
from requests.structures import CaseInsensitiveDict

class FinanceWithdrawalsStream(Stream):
    name = "tiktok_finance_withdrawals"
    primary_keys = ["id"]
    replication_key = "create_time"
    schema = th.PropertiesList(
        th.Property("amount", th.StringType),
        th.Property("create_time", th.IntegerType),
        th.Property("currency", th.StringType),
        th.Property("id", th.StringType),
        th.Property("status", th.StringType),
        th.Property("type", th.StringType) 
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, any]]:

        withdrawals_url = self.config['base_url']+'/finance/202309/withdrawals'
        timestamp = str(int(time.time()))
       
        params = {
            'app_key': self.config['app_key'],
            'access_token': self.config['access_token'],
            'timestamp': timestamp,
            'types': "['WITHDRAW','SETTLE','TRANSFER','REVERSE']",
            "sort_field": "statement_time",
            "shop_cipher": "TTP_tD0FYAAAAABIH2BTJJSHZoaLly5In-qW",
            "version":202309
        }
        headers = {
            'x-tts-access-token': self.config['access_token'],
            "Content-Type": "application/json"
        }

        signature = generate_signature(withdrawals_url, params,self.config['app_secret'])
        params['sign'] = signature
        
        # Form the complete URL with parameters
        full_url = f"{withdrawals_url}?{urlencode(params)}"


        # # Set headers
        # headers = CaseInsensitiveDict()
        # headers["x-tts-access-token"] = self.config['access_token']

        # Make the GET request
        response = requests.get(full_url, headers=headers)
       
        if response.status_code == 401:
            self.logger.error("Unauthorized access - check your access token.")
        response.raise_for_status()
        data = response.json()
        if data["message"] != "Success":
            raise Exception(f"Error: {data['message']}")
        else:
            for withdrawal in data["data"]["withdrawals"]:
                yield withdrawal
