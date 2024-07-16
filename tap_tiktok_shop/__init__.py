# #!/usr/bin/env python3
# import os
# import json
# import singer
# import requests
# from singer import utils
# from singer import metadata
# # from singer.catalog import Catalog, CatalogEntry
# # from singer import Schema
# from tap_tiktok_shop.context import Context
# from tap_tiktok_shop.exceptions import TikTokError

# from tap_tiktok_shop.streams.tiktok_shop_orders import TikTokShopOrdersStream
# from tap_tiktok_shop.streams.tiktok_shops import TikTokShops


# REQUIRED_CONFIG_KEYS = ["client_id", "client_secret", "redirect_uri", "access_token", "refresh_token"]
# # REQUIRED_CONFIG_KEYS = ["access_token", "api_base_url"]
# LOGGER = singer.get_logger()

# def get_abs_path(path):
#     return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# # Load schemas from schemas folder
# def load_schemas():
#     schemas = {}
#     for filename in os.listdir(get_abs_path('schemas')):
#         path = get_abs_path('schemas') + '/' + filename
#         schema_name = filename.replace('.json', '')
#         with open(path) as file:
#             schemas[schema_name] = json.load(file)

#     return schemas


# def discover():
#     raw_schemas = load_schemas()
#     streams = []
#     print(Context.stream_objects)

#     for schema_name, schema in raw_schemas.items():
#         if schema_name not in Context.stream_objects:
#             print(f"Skipping {schema_name} as it is not a valid stream")
#             continue

#         stream = Context.stream_objects[schema_name]()

#         # create and add catalog entry
#         catalog_entry = {
#             'stream': schema_name,
#             'tap_stream_id': schema_name,
#             'schema': schema,
#             'metadata' : stream.metadata,
#             'key_properties': stream.key_properties,
#             'replication_key': stream.replication_key,
#             'replication_method': stream.replication_method
#         }
#         streams.append(catalog_entry)

#     return {'streams': streams}


# def sync(config, state, catalog):
#     """ Sync data from tap source """
#     # Loop over selected streams in catalog


#     selected_streams = catalog.get_selected_streams(state)
#     print("Selected Streams:", [stream.tap_stream_id for stream in selected_streams])



#     for stream in catalog.get_selected_streams(state):
#         # LOGGER.info("Syncing stream:" + stream.tap_stream_id)

#         bookmark_column = stream.replication_key
#         is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

#         singer.write_schema(
#             stream_name=stream.tap_stream_id,
#             schema=stream.schema,
#             key_properties=stream.key_properties,
#         )

#         if stream.tap_stream_id == 'tiktok_shop_orders':
#             print("TikTok Shop Orders Stream")
#             stream_instance = TikTokShopOrdersStream(config)
#         elif stream.tap_stream_id == 'tiktok_shop_products':
#             stream_instance = TikTokShops(config)
#             ## TODO Add Finance Stream
#         else:
#             raise Exception(f"Unsupported stream: {stream.tap_stream_id}")

#         rows = stream_instance.get_records(None)
#         for row in rows:
#             singer.write_records(stream.tap_stream_id, [row])
#             if stream.replication_key:
#                 singer.write_state({stream.tap_stream_id: row[stream.replication_key]})

#     return


# @utils.handle_top_exception(LOGGER)
# def main():
#     # Parse command line arguments
#     args = utils.parse_args(REQUIRED_CONFIG_KEYS)

#     # If discover flag was passed, run discovery mode and dump output to stdout
#     if args.discover:
#         catalog = discover()
#         print(json.dumps(catalog, indent=2))
#     # Otherwise run in sync mode
#     else:
#         Context.tap_start = utils.now()
#         if args.catalog:
#             Context.catalog = args.catalog.to_dict()
#         else:
#             Context.catalog = discover()

#         Context.config = args.config
#         Context.state = args.state
#         sync()

# if __name__ == "__main__":
#     main()



# import requests
# from singer_sdk import Tap, Stream
# from singer_sdk import typing as th
# from tap_tiktok_shop.streams import TikTokShopOrdersStream, TikTokShops

# class TapTikTokShop(Tap):
#     name = "tap-tiktok-shop"

#     config_jsonschema = th.PropertiesList(
#         th.Property("access_token", th.StringType, required=False),
#         th.Property("base_url", th.StringType, required=False)
#     ).to_dict()

#     def discover_streams(self) -> list[Stream]:
#         print("srikar")
#         #print(f"Configuration: {self.config}") 
#         return [
#             #TikTokShopOrdersStream(tap=self),
#             TikTokShops(tap=self)
#         ]

# def main():
#     TapTikTokShop.cli()

# if __name__ == "__main__":
#     main()




import requests
import sys
import json
import os
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
            #TikTokShopOrdersStream(tap=self),
            TikTokShops(tap=self)
        ]

def load_config(config_path):
    with open(config_path) as config_file:
        return json.load(config_file)

def main():
    import argparse

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

