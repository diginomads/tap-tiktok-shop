import requests
import singer_sdk
from singer_sdk import Stream
from singer_sdk import typing as th
from typing import Optional, Iterable, Dict
import json, time
from tap_tiktok_shop import generate_signature
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

        settlements_url = self.config['base_url']+'/finance/202309/statements'
        timestamp = str(int(time.time()))
       
        params = {
            'app_key': self.config['app_key'],
            'access_token': self.config['access_token'],
            'shop_id': '7495660122206341889',
            'timestamp': timestamp,
            'version': '202212',
        }
        headers = {
            'x-tts-access-token': self.config['access_token'],
            "Content-Type": "application/json"
        }

        data = {
            "sort_field": "statement_time",
        }

        signature = generate_signature(settlements_url, params,self.config['app_secret'])
        params['sign'] = signature
        
        # Form the complete URL with parameters
        full_url = f"{settlements_url}?{urlencode(params)}"


        # # Set headers
        # headers = CaseInsensitiveDict()
        # headers["x-tts-access-token"] = self.config['access_token']

        # Make the POST request
        response = requests.get(full_url, headers=headers, data=data)
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
