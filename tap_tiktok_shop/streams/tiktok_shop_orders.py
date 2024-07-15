import requests
from tap_tiktok_shop.context import Context
from tap_tiktok_shop.streams.base import Stream

class TikTokShopOrdersStream(Stream):
    name = "tiktok_shop_orders"
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

    def get_records(self):
        headers = {
            "Authorization": f"Bearer {self.config['access_token']}",
            "Content-Type": "application/json"
        }
        params = {
            "shop_cipher": "",
            "ids":""
        }

        response = requests.get("https://open-api.tiktok.com/api/order/202309/orders", headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        for order in data["data"]["orders"]:
            yield order

Context.stream_objects['tiktok_shop_orders'] = TikTokShopOrdersStream