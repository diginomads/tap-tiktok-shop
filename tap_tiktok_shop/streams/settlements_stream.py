import requests
import singer_sdk
from singer_sdk import Stream
from singer_sdk import typing as th
from typing import Optional, Iterable, Dict

class SettlementsStream(Stream):
    name = "tiktok_settlements"
    primary_keys = ["order_id"]
    replication_key = "update_time"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("status", th.StringType),
        th.Property("buyer_email", th.StringType),
        th.Property("buyer_message", th.StringType),
        th.Property("product_id", th.StringType),
        th.Property("cancel_reason", th.StringType),
        th.Property("user_id", th.StringType),
        th.Property("update_time", th.IntegerType)
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, any]]:
        headers = {
            'x-tts-access-token': self.config['access_token'],
            "Content-Type": "application/json"
        }
        params = {
            
        }

        response = requests.get("https://open-api.tiktok.com/api/order/202309/orders", headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        for order in data["data"]["orders"]:
            yield order