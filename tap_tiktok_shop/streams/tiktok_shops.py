from singer_sdk import typing as th
from singer_sdk.streams import Stream
import requests
from tap_tiktok_shop.context import Context

class TikTokShops(Stream):
    name = "tiktok_shops"
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("cipher", th.StringType),
        th.Property("code", th.StringType),
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("region", th.StringType),
        th.Property("seller_type", th.StringType)
    ).to_dict()

    def get_records(self):
        headers = {
            "Authorization": f"Bearer {self.config['access_token']}",
            "Content-Type": "application/json"
        }

        response = requests.get("https://open-api.tiktok.com/api/orders/202309/shops", headers=headers)
        response.raise_for_status()
        data = response.json()
        if data["message"] != "Success":
            raise Exception(f"Error: {data['message']}")
        else:
            for shop in data["data"]["shops"]:
                yield shop

Context.stream_objects['tiktok_shops'] = TikTokShops
