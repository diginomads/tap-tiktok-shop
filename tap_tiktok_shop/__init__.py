import requests
import sys
import json
import os
import argparse
from singer_sdk import Tap, Stream
from singer_sdk import typing as th
from tap_tiktok_shop.streams import TikTokShops

class TapTikTokShop(Tap):
    name = "tap-tiktok-shop"

    config_jsonschema = th.PropertiesList(
        th.Property("access_token", th.StringType, required=True),
        th.Property("base_url", th.StringType, required=True)
    ).to_dict()

    
    def discover_streams(self) -> list[Stream]:
        self.logger.info(f"Configuration: {self.config}")
        return [
            TikTokShops(tap=self)
            #TikTokShopOrdersStream(tap=self),
        ]

def load_config(config_path):
    with open(config_path) as config_file:
        return json.load(config_file)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required=True, help='Configuration file')
    parser.add_argument('--discover', action='store_true', help='Run discovery mode')

    args = parser.parse_args()

    config = load_config(args.config)

    tap = TapTikTokShop(config=config)

    if args.discover:
        catalog = tap.discover()
        print(json.dumps(catalog, indent=2))
    else:
        tap.sync_all()

if __name__ == "__main__":
    main()

