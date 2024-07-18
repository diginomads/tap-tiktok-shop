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

class FinanceStatementsStream(Stream):
    name = "tiktok_finance_statements"
    primary_keys = ["id"]
    replication_key = "statement_time"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("adjustment_amount", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("fee_amount", th.StringType),
        th.Property("payment_id", th.StringType),
        th.Property("payment_status", th.StringType),
        th.Property("revenue_amount", th.StringType),
        th.Property("settlement_amount", th.StringType),
        th.Property("statement_time", th.IntegerType),

        
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, any]]:

        statements_url = self.config['base_url']+'/finance/202309/statements'
        timestamp = str(int(time.time()))
       
        params = {
            'app_key': self.config['app_key'],
            'access_token': self.config['access_token'],
            'timestamp': timestamp,
            "sort_field": "statement_time",
            "shop_cipher": "TTP_tD0FYAAAAABIH2BTJJSHZoaLly5In-qW",
            "version":202309
        }
        headers = {
            'x-tts-access-token': self.config['access_token'],
            "Content-Type": "application/json"
        }

        signature = generate_signature(statements_url, params,self.config['app_secret'])
        params['sign'] = signature
        
        # Form the complete URL with parameters
        full_url = f"{statements_url}?{urlencode(params)}"


        # # Set headers
        # headers = CaseInsensitiveDict()
        # headers["x-tts-access-token"] = self.config['access_token']

        # Make the GET request
        response = requests.get(full_url, headers=headers)
        # Print the full request object
        print("headers: ", response.request.headers)
        print("body: ", response.request.body)
        print("url: ", response.request.url)

        print("response:", response.text)

        if response.status_code == 401:
            self.logger.error("Unauthorized access - check your access token.")
        response.raise_for_status()
        data = response.json()
        if data["message"] != "Success":
            raise Exception(f"Error: {data['message']}")
        else:
            for statement in data["data"]["statements"]:
                yield statement
