import os
from pathlib import Path
from typing import Dict

from dotenv import load_dotenv
import requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from py_clob_client.headers.headers import create_level_1_headers
from py_clob_client.signer import Signer

ENV_PATH = Path(".env")


def load_env() -> None:
    if ENV_PATH.exists():
        load_dotenv(ENV_PATH)
    else:
        load_dotenv()


def update_env_file(updates: Dict[str, str]) -> None:
    if not updates:
        return

    lines = []
    existing = {}
    if ENV_PATH.exists():
        lines = ENV_PATH.read_text().splitlines()
        for idx, line in enumerate(lines):
            if not line or line.strip().startswith("#") or "=" not in line:
                continue
            key, _ = line.split("=", 1)
            existing[key.strip()] = idx

    for key, value in updates.items():
        entry = f"{key}={value}"
        if key in existing:
            lines[existing[key]] = entry
        else:
            lines.append(entry)

    ENV_PATH.write_text("\n".join(lines) + "\n")


def mask(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return value[:4] + "..." + value[-4:]


def fetch_l2_creds_direct(host: str, signer: Signer, nonce: int = 0) -> ApiCreds:
    headers = create_level_1_headers(signer, nonce)
    create_url = f"{host}/auth/api-key"
    derive_url = f"{host}/auth/derive-api-key"

    try:
        resp = requests.post(create_url, headers=headers, timeout=20)
        if resp.status_code == 200:
            payload = resp.json()
            return ApiCreds(
                api_key=payload["apiKey"],
                api_secret=payload["secret"],
                api_passphrase=payload["passphrase"],
            )
    except Exception:
        pass

    resp = requests.get(derive_url, headers=headers, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    return ApiCreds(
        api_key=payload["apiKey"],
        api_secret=payload["secret"],
        api_passphrase=payload["passphrase"],
    )


def main() -> None:
    load_env()

    private_key = os.getenv("PRIVATE_KEY") or os.getenv("POLYGON_PRIVATE_KEY")
    if not private_key:
        raise SystemExit("Missing PRIVATE_KEY / POLYGON_PRIVATE_KEY in .env")

    host = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
    chain_id = int(os.getenv("CLOB_CHAIN_ID", "137"))
    signature_type = int(os.getenv("CLOB_SIGNATURE_TYPE", "0"))
    funder = os.getenv("CLOB_FUNDER")

    signer = Signer(private_key, chain_id)

    nonce = int(os.getenv("CLOB_L1_NONCE", "0"))
    print("Generating L1 signature and calling /auth/api-key directly...")
    creds = fetch_l2_creds_direct(host, signer, nonce)

    update_env_file(
        {
            "CLOB_API_KEY": creds.api_key,
            "CLOB_API_SECRET": creds.api_secret,
            "CLOB_API_PASSPHRASE": creds.api_passphrase,
        }
    )
    print("CLOB L2 credentials saved to .env (masked below).")

    print(f"CLOB_API_KEY: {mask(creds.api_key)}")
    print(f"CLOB_API_SECRET: {mask(creds.api_secret)}")
    print(f"CLOB_API_PASSPHRASE: {mask(creds.api_passphrase)}")

    # Verify L2 auth by listing API keys
    client = ClobClient(
        host=host,
        chain_id=chain_id,
        key=private_key,
        signature_type=signature_type,
        funder=funder,
    )
    client.set_api_creds(creds)
    keys = client.get_api_keys()
    print(f"L2 auth OK. Active API keys: {len(keys) if keys else 0}")


if __name__ == "__main__":
    main()
