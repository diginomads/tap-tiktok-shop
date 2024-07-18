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

class FinancePaymentsStream(Stream):
    name = "tiktok_finance_payments"
    primary_keys = ["id"]
    replication_key = "paid_time"
    schema = th.PropertiesList(
        th.Property("amount", th.ObjectType(
                    th.Property("currency", th.StringType),
                    th.Property("value", th.StringType)
                )),
        th.Property("bank_account", th.StringType),
        th.Property("create_time", th.IntegerType),
        th.Property("exchange_rate", th.StringType),
        th.Property("id", th.StringType),
        th.Property("paid_time", th.IntegerType),
        th.Property("payment_amount_before_exchange", th.ObjectType(
            th.Property("currency", th.StringType),
            th.Property("value", th.StringType)
        )),
        th.Property("reserve_amount", th.ObjectType(
            th.Property("currency", th.StringType),
            th.Property("value", th.StringType)
        )),
        th.Property("settlement_amount", th.ObjectType(
            th.Property("currency", th.StringType),
            th.Property("value", th.StringType)
        )),
        th.Property("status", th.StringType)
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, any]]:

        payments_url = self.config['base_url']+'/finance/202309/payments'
        timestamp = str(int(time.time()))
       
        params = {
            'app_key': self.config['app_key'],
            'access_token': self.config['access_token'],
            'timestamp': timestamp,
            "sort_field": "create_time",
            "shop_cipher": "TTP_tD0FYAAAAABIH2BTJJSHZoaLly5In-qW",
            "version":202309
        }
        headers = {
            'x-tts-access-token': self.config['access_token'],
            "Content-Type": "application/json"
        }

        signature = generate_signature(payments_url, params,self.config['app_secret'])
        params['sign'] = signature
        
        # Form the complete URL with parameters
        full_url = f"{payments_url}?{urlencode(params)}"


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
            for payment in data["data"]["payments"]:
                yield payment
