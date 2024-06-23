#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-tiktok-shop",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_tiktok_shop"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-tiktok-shop=tap_tiktok_shop:main
    """,
    packages=["tap_tiktok_shop"],
    package_data = {
        "schemas": ["tap_tiktok_shop/schemas/*.json"]
    },
    include_package_data=True,
)
