#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="tap-tiktok-shop",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=[
        "singer-python",
        "requests",
        "singer-sdk",
        "jsonschema"
    ],
    entry_points="""
    [console_scripts]
    tap-tiktok-shop=tap_tiktok_shop.__init__:main
    """,
    packages=find_packages(),
    package_data={
        "tap_tiktok_shop": ["schemas/*.json"]
    },
    include_package_data=True,
)
