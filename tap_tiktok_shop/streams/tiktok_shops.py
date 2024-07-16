# from singer_sdk import typing as th
# from singer_sdk.streams import Stream
# import requests
# from typing import Optional, Iterable, Dict
# #from tap_tiktok_shop.context import Context

# class TikTokShops(Stream):
#     name = "tiktok_shops"
#     primary_keys = ["id"]
#     schema = th.PropertiesList(
#         th.Property("cipher", th.StringType),
#         th.Property("code", th.StringType),
#         th.Property("id", th.StringType),
#         th.Property("name", th.StringType),
#         th.Property("region", th.StringType),
#         th.Property("seller_type", th.StringType)
#     ).to_dict()

#     def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, any]]:
#         print("srikar")
#         headers = {
#             "Authorization": f"Bearer {"TTP_pXPxLAAAAADQviXu4klyvPIVyjAP4Y8zmpnVOKqrvl44M7PhNBemneftHlqY7CqWO2QbKZAUobuw5WqO5lGekPJn1HBKg7GpcUskRanv5mjETdKssETcfHM9yL5XeMrep8kn9pTpdoc1spwb7GksbGYaQOnBsx1TuOX3qYzlI-Tj3O3PsJmq6A"}",
#             "Content-Type": "application/json"
#         }

#         params = {
#         'app_key': '29a39d'
#         }

#         response = requests.get("https://open-api.tiktokglobalshop.com/seller/202309/shops", headers=headers,params=params)
#         response.raise_for_status()
#         data = response.json()
#         if data["message"] != "Success":
#             raise Exception(f"Error: {data['message']}")
#         else:
#             for shop in data["data"]["shops"]:
#                 yield shop

# #Context.stream_objects['tiktok_shops'] = TikTokShops



# import requests
# from singer_sdk import Stream
# from singer_sdk import typing as th
# from typing import Optional, Iterable, Dict

# class TikTokShops(Stream):
#     name = "tiktok_shops"
#     primary_keys = ["id"]
#     schema = th.PropertiesList(
#         th.Property("cipher", th.StringType),
#         th.Property("code", th.StringType),
#         th.Property("id", th.StringType),
#         th.Property("name", th.StringType),
#         th.Property("region", th.StringType),
#         th.Property("seller_type", th.StringType)
#     ).to_dict()

#     def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, any]]:
#         headers = {
#             "x-tts-access-token": f"{self.config['access_token']}",
#             "Content-Type": "application/json"
#         }
        
#         params = {
#         'sign': 'b07a01954b99e632c85310e346ca363c405e07bd374ae5299957adbe8fded33a',
#         'timestamp': '1721154721',
#         'app_key': '6bs939tdu2pip'
#         }

#         url = f"{self.config['base_url']}/seller/202309/shops?"
#         self.logger.info(f"Requesting URL: {url}")

#         response = requests.get(url, headers=headers,params=params)
#         if response.status_code == 401:
#             self.logger.error("Unauthorized access - check your access token.")
#         response.raise_for_status()
#         data = response.json()
#         if data["message"] != "Success":
#             raise Exception(f"Error: {data['message']}")
#         else:
#             for shop in data["data"]["shops"]:
#                 yield shop


import requests
import hashlib
import time
from singer_sdk import Stream
from singer_sdk import typing as th
from typing import Optional, Iterable, Dict

class TikTokShops(Stream):
    name = "tiktok_shops"
    primary_keys = ["id"]
    schema = th.PropertiesList(
        #th.Property("cipher", th.StringType),
        #th.Property("code", th.StringType),
        th.Property("id", th.StringType),
        #th.Property("name", th.StringType),
        th.Property("region", th.StringType),
        #th.Property("seller_type", th.StringType)
    ).to_dict()

    def generate_sign(self, app_key, secret_key, timestamp):
        raw_string = f"app_key={app_key}&timestamp={timestamp}&secret_key={secret_key}"
        hash_object = hashlib.sha256(raw_string.encode())
        return hash_object.hexdigest()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, any]]:
        app_key = "6bs939tdu2pip"  # Replace with your actual app key
        secret_key = "92c8b73d6467f557bf21c617cdb24d5600ea8316"  # Replace with your actual secret key
        timestamp = str(int(time.time()))  # Current timestamp
        sign = self.generate_sign(app_key, secret_key, timestamp)

        headers = {
            "Content-Type": "application/json",
            "x-tts-access-token": self.config['access_token'],
        }

        params = {
            'access_token': self.config['access_token'],
            'app_key': app_key,
            'shop_id': '',
            'sign': sign,
            'timestamp': timestamp,
            'version': '202309'
            
        }


        url = f"{self.config['base_url']}/seller/202309/shops"
        self.logger.info(f"Requesting URL: {url}")

        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 401:
            self.logger.error("Unauthorized access - check your access token.")
        response.raise_for_status()
        data = response.json()
        if data["message"] != "Success":
            raise Exception(f"Error: {data['message']}")
        else:
            for shop in data["data"]["shops"]:
                yield shop
