#!/usr/bin/env python3
"""Runtime entry point for GitHub Actions.

The STOXX web server may occasionally present a certificate chain that the
GitHub-hosted Linux runner cannot validate. This entry point first forces
Python/aiohttp to use the current certifi CA bundle. If certificate validation
still fails, it retries only official stoxx.com URLs with certificate checking
disabled. FT requests remain fully verified.
"""

from __future__ import annotations

import asyncio
import os
from urllib.parse import urlparse

import certifi

# aiohttp creates its verified SSL context during import. Set the CA bundle
# before importing aiohttp or Indexes.
os.environ["SSL_CERT_FILE"] = certifi.where()

import aiohttp  # noqa: E402
import Indexes  # noqa: E402


async def get_text_with_stoxx_ssl_fallback(
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, object] | None = None,
) -> str:
    """Download text normally, then retry only STOXX on certificate failure."""
    try:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.text()
    except (
        aiohttp.ClientConnectorCertificateError,
        aiohttp.ClientSSLError,
    ) as exc:
        host = (urlparse(url).hostname or "").lower()
        is_official_stoxx = host == "stoxx.com" or host.endswith(".stoxx.com")

        if not is_official_stoxx:
            raise

        print(
            f"  Avertissement TLS pour {host}: {exc}. "
            "Nouvelle tentative STOXX sans validation du certificat."
        )
        async with session.get(url, params=params, ssl=False) as response:
            response.raise_for_status()
            return await response.text()


# Keep the data-processing code unchanged and replace only its HTTP helper.
Indexes.get_text = get_text_with_stoxx_ssl_fallback
Indexes.VERSION = "11.1.0"


if __name__ == "__main__":
    asyncio.run(Indexes.main())
