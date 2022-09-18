"""
Contains getml_utils installation specifications
"""

from setuptools import setup  # type: ignore

setup(
    name="getml_uploader",
    version="0.1.0",
    py_modules=["getml_uploader"],
    install_requires=[
        "click==8.1.3",
        "google-crc32c==1.1.5",
        "google-cloud-storage==2.5.0",
        "google-api-python-client==2.61.0",
        "httplib2==0.20.4",
        "oauth2client==4.1.3",
        "protobuf==4.21.6",
    ],
    entry_points={
        "console_scripts": [
            "getml_uploader=getml_uploader:upload",
        ],
    },
)
