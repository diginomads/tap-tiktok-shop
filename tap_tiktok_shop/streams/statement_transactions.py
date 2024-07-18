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

class StatementTransactionsStream(Stream):
    name = "tiktok_statement_transactions"
    primary_keys = ["id"]
    replication_key = "order_create_time"
    schema = th.PropertiesList(
        th.Property("actual_return_shipping_fee_amount", th.StringType),
        th.Property("actual_shipping_fee_amount", th.StringType),
        th.Property("adjustment_amount", th.StringType),
        th.Property("adjustment_id", th.StringType),
        th.Property("adjustment_order_id", th.StringType),
        th.Property("affiliate_commission_amount", th.StringType),
        th.Property("affiliate_commission_before_pit", th.StringType),
        th.Property("affiliate_partner_commission_amount", th.StringType),
        th.Property("after_seller_discounts_subtotal_amount", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("customer_order_refund_amount", th.StringType),
        th.Property("customer_payment_amount", th.StringType),
        th.Property("customer_refund_amount", th.StringType),
        th.Property("customer_shipping_fee_amount", th.StringType),
        th.Property("fbm_shipping_cost_amount", th.StringType),
        th.Property("fbt_shipping_cost_amount", th.StringType),
        th.Property("fee_amount", th.StringType),
        th.Property("id", th.StringType),
        th.Property("order_create_time", th.IntegerType),
        th.Property("order_id", th.StringType),
        th.Property("pit_amount", th.StringType),
        th.Property("platform_commission_amount", th.StringType),
        th.Property("platform_discount_amount", th.StringType),
        th.Property("platform_discount_refund_amount", th.StringType),
        th.Property("platform_refund_subsidy_amount", th.StringType),
        th.Property("platform_shipping_fee_discount_amount", th.StringType),
        th.Property("referral_fee_amount", th.StringType),
        th.Property("refund_administration_fee_amount", th.StringType),
        th.Property("refund_shipping_cost_discount_amount", th.StringType),
        th.Property("retail_delivery_fee_amount", th.StringType),
        th.Property("retail_delivery_fee_payment_amount", th.StringType),
        th.Property("retail_delivery_fee_refund_amount", th.StringType),
        th.Property("revenue_amount", th.StringType),
        th.Property("sales_tax_amount", th.StringType),
        th.Property("sales_tax_payment_amount", th.StringType),
        th.Property("sales_tax_refund_amount", th.StringType),
        th.Property("settlement_amount", th.StringType),
        th.Property("shipping_cost_discount_amount", th.StringType),
        th.Property("shipping_fee_amount", th.StringType),
        th.Property("shipping_fee_subsidy_amount", th.StringType),
        th.Property("shipping_insurance_fee_amount", th.StringType),
        th.Property("signature_confirmation_fee_amount", th.StringType),
        th.Property("transaction_fee_amount", th.StringType),
        th.Property("type", th.StringType)
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, any]]:

        statement_id = ""
        payments_url = self.config['base_url']+'/finance/202309/'+statement_id+'/statement_transactions'
        timestamp = str(int(time.time()))
       
        params = {
            'app_key': self.config['app_key'],
            'access_token': self.config['access_token'],
            'timestamp': timestamp,
            "sort_field": "order_create_time",
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
        
        print("response:", response.text)

        if response.status_code == 401:
            self.logger.error("Unauthorized access - check your access token.")
        response.raise_for_status()
        data = response.json()
        if data["message"] != "Success":
            raise Exception(f"Error: {data['message']}")
        else:
            for transaction in data["data"]["statement_transactions"]:
                yield transaction
