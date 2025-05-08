import os
import base64
import time
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

load_dotenv()

ENV = os.getenv("ENV", "DEMO")
KEY_ID = os.getenv(f"{ENV}_KEYID")
KEY_FILE = os.getenv(f"{ENV}_KEYFILE")
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

with open(KEY_FILE, "rb") as f:
    PRIVATE_KEY = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())

def sign_request(method, full_path):
    timestamp = str(int(time.time() * 1000))
    msg = timestamp + method.upper() + full_path
    signature = base64.b64encode(
        PRIVATE_KEY.sign(
            msg.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
    ).decode("utf-8")
    
    return {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "accept": "application/json",
        "content-type": "application/json"
    }