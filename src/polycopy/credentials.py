from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable
from typing import TYPE_CHECKING, Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds

if TYPE_CHECKING:
    from .config import Settings

ClobClientFactory = Callable[..., ClobClient]


def derive_or_create_api_credentials(
    *,
    private_key: str,
    rest_url: str,
    chain_id: int,
    signature_type: int,
    nonce: int | None = None,
    client_factory: ClobClientFactory = ClobClient,
) -> ApiCreds:
    """Create or derive CLOB API credentials using an L1 private key."""
    client = client_factory(host=rest_url, chain_id=chain_id, key=private_key, signature_type=signature_type)
    creds = client.create_or_derive_api_creds(nonce=nonce)
    if creds is None:
        raise RuntimeError("Failed to obtain Polymarket API credentials from CLOB")
    if not creds.api_key or not creds.api_secret:
        raise RuntimeError("Incomplete Polymarket API credentials returned from CLOB (missing key/secret)")
    return creds


def require_api_credentials(settings: "Settings") -> None:
    """Fail fast if API credentials are missing."""
    if not settings.api_key or not settings.api_secret:
        raise SystemExit(
            "Missing required Polymarket API credentials; provide API key/secret or derive them from PRIVATE_KEY"
        )


def ensure_api_credentials(
    settings: "Settings",
    *,
    nonce: int | None = None,
    client_factory: ClobClientFactory = ClobClient,
) -> ApiCreds:
    """Populate API credentials on the provided settings if absent."""
    if settings.api_key and settings.api_secret:
        return ApiCreds(
            api_key=settings.api_key,
            api_secret=settings.api_secret,
            api_passphrase=settings.api_passphrase or "",
        )

    creds = derive_or_create_api_credentials(
        private_key=settings.private_key,
        rest_url=settings.clob_rest_url,
        chain_id=settings.chain_id,
        signature_type=settings.signature_type,
        nonce=nonce,
        client_factory=client_factory,
    )
    settings.api_key = creds.api_key
    settings.api_secret = creds.api_secret
    settings.api_passphrase = creds.api_passphrase
    return creds


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create or derive Polymarket API credentials using a private key"
    )
    parser.add_argument(
        "--private-key",
        help="L1 private key used to sign the credential request (defaults to PRIVATE_KEY env)",
        default=os.getenv("PRIVATE_KEY"),
    )
    parser.add_argument("--clob-rest-url", default="https://clob.polymarket.com", help="CLOB host")
    parser.add_argument("--chain-id", type=int, default=137, help="Chain ID for signing (default 137)")
    parser.add_argument(
        "--signature-type",
        type=int,
        default=1,
        help="Signature type: 1 for Magic/email proxy, 2 for Web3 browser wallet proxy",
    )
    parser.add_argument("--nonce", type=int, help="Optional nonce to use when deriving credentials")
    parser.add_argument(
        "--env-format",
        action="store_true",
        help="Print credentials in shell export format instead of JSON-like lines",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not args.private_key:
        parser.error("private key is required (--private-key or PRIVATE_KEY env)")

    creds = derive_or_create_api_credentials(
        private_key=args.private_key,
        rest_url=args.clob_rest_url,
        chain_id=args.chain_id,
        signature_type=args.signature_type,
        nonce=args.nonce,
    )

    if args.env_format:
        print(f"API_KEY={creds.api_key}")
        print(f"API_SECRET={creds.api_secret}")
        print(f"API_PASSPHRASE={creds.api_passphrase}")
    else:
        print(
            json.dumps(
                {
                    "api_key": creds.api_key,
                    "api_secret": creds.api_secret,
                    "api_passphrase": creds.api_passphrase,
                }
            )
        )


if __name__ == "__main__":  # pragma: no cover
    main()
