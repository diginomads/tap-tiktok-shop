#!/usr/bin/env python3
import os
import json
import singer
import requests
from singer import utils, metadata
# from singer.catalog import Catalog, CatalogEntry
# from singer import Schema
from tap_tiktok_shop.context import Context
from tap_tiktok_shop.exceptions import TikTokError

REQUIRED_CONFIG_KEYS = ["client_id", "client_secret", "redirect_uri", "access_token", "refresh_token"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        schema_name = filename.replace('.json', '')
        with open(path) as file:
            schemas[schema_name] = json.load(file)

    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        if stream_id == 'tiktok_shop_orders':
            key_properties = ['id']
            replication_key = 'update_time'
        elif stream_id == 'tiktok_shops':
            key_properties = ['id']
            replication_key = '' # No replication key
        else:
            raise Exception(f"Unsupported stream: {stream_id}")
        
        stream_metadata = metadata.get_standard_metadata(
        schema=schema.to_dict(),
        key_properties=key_properties,
        valid_replication_keys=[replication_key]
        )
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=replication_key,
                is_view=False,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method='INCREMENTAL',
            )
        )
    return streams


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog


    selected_streams = catalog.get_selected_streams(state)
    print("Selected Streams:", [stream.tap_stream_id for stream in selected_streams])



    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema,
            key_properties=stream.key_properties,
        )

        if stream.tap_stream_id == 'tiktok_shop_orders':
            print("TikTok Shop Orders Stream")
            stream_instance = TikTokShopOrdersStream(config)
        elif stream.tap_stream_id == 'tiktok_shop_products':
            stream_instance = TikTokShops(config)
            ## TODO Add Finance Stream
        else:
            raise Exception(f"Unsupported stream: {stream.tap_stream_id}")

        rows = stream_instance.get_records(None)
        for row in rows:
            singer.write_records(stream.tap_stream_id, [row])
            if stream.replication_key:
                singer.write_state({stream.tap_stream_id: row[stream.replication_key]})

    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()