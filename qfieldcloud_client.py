from config import CONFIG
from qfieldcloud_sdk.sdk import Client

def get_client():
    return Client(
        url="https://app.qfield.cloud/api/v1/",
        token=CONFIG["qfieldcloud"]["token"],
        verify_ssl=True,
    )