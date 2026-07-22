#!/usr/bin/env python3
"""Runtime entry point using the last successful STOXX transport.

FT requests keep using the curl_cffi transport defined in Indexes.py.
Official STOXX requests use aiohttp. They are attempted first with the current
certifi CA bundle and retried with certificate verification disabled only when
STOXX presents the certificate-chain error observed on GitHub-hosted runners.
"""

from __future__ import annotations

import asyncio
import ssl
from urllib.parse import urlparse

import aiohttp
import certifi

import Indexes


ORIGINAL_GET_TEXT = Indexes.get_text


async def get_stoxx_text(url: str, params: dict[str, object] | None = None) -> str:
    """Fetch one official STOXX URL with the transport that previously worked."""
    timeout = aiohttp.ClientTimeout(total=180, connect=30, sock_read=120)
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(
        limit=8,
        ttl_dns_cache=300,
        ssl=ssl_context,
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0 Safari/537.36"
        ),
        "Accept": "text/html,text/plain,*/*",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://stoxx.com/",
    }

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers=headers,
    ) as session:
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                return await response.text()
        except (
            aiohttp.ClientConnectorCertificateError,
            aiohttp.ClientSSLError,
        ) as exc:
            print(
                f"  Avertissement TLS STOXX pour {urlparse(url).hostname}: {exc}. "
                "Nouvelle tentative sans validation du certificat."
            )
            async with session.get(url, params=params, ssl=False) as response:
                response.raise_for_status()
                return await response.text()


async def hybrid_get_text(
    session: object,
    url: str,
    params: dict[str, object] | None = None,
) -> str:
    """Route only official STOXX URLs through aiohttp."""
    host = (urlparse(url).hostname or "").lower()
    is_stoxx = host == "stoxx.com" or host.endswith(".stoxx.com")

    if is_stoxx:
        return await get_stoxx_text(url, params)

    return await ORIGINAL_GET_TEXT(session, url, params)


Indexes.get_text = hybrid_get_text
Indexes.VERSION = "12.1.0"


if __name__ == "__main__":
    asyncio.run(Indexes.main())
