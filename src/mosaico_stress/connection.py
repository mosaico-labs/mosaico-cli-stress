"""Build MosaicoClient connection kwargs from environment variables.

Env vars:
    MOSAICO_DAEMON_URL   host or host:port (default port 6276)
    MOSAICO_API_KEY      bearer token
    MOSAICO_TLS          any value → enable TLS
    MOSAICO_CERT_PATH    custom CA cert path
"""

from __future__ import annotations

import os
from typing import List
from urllib.parse import urlparse

from mosaicolabs import MosaicoClient, QueryResponse, QueryTopic


def get_connect_kwargs() -> dict:
    """Return kwargs dict ready for MosaicoClient.connect(**kwargs)."""
    daemon_url = os.environ.get("MOSAICO_DAEMON_URL", "")
    if not daemon_url:
        raise EnvironmentError(
            "MOSAICO_DAEMON_URL is required (e.g. 'api.mosaico.dev' or 'api.mosaico.dev:6276')."
        )

    host, port = _parse_url(daemon_url)

    kwargs: dict = {
        "host": host,
        "port": port,
    }

    api_key = os.environ.get("MOSAICO_API_KEY")
    if api_key:
        kwargs["api_key"] = api_key

    if os.environ.get("MOSAICO_TLS"):
        kwargs["enable_tls"] = True
        cert_path = os.environ.get("MOSAICO_CERT_PATH")
        if cert_path:
            kwargs["tls_cert_path"] = cert_path

    return kwargs


def _parse_url(url: str) -> tuple[str, int]:
    if "://" not in url:
        url = f"tcp://{url}"
    parsed = urlparse(url)
    return parsed.hostname or "localhost", parsed.port or 6276


def discover_resources(connect_kwargs: dict) -> List[str]:
    """Return list of 'sequence_name/topic_name' strings from the platform."""
    resources: List[str] = []

    with MosaicoClient.connect(**connect_kwargs) as sdk_client:
        query = QueryTopic().with_name_match(".*")
        results: QueryResponse = sdk_client.query(query)

        for item in results:
            for topic in item.topics:
                handler = sdk_client.topic_handler(item.sequence.name, topic.name)
                if handler:
                    resources.append(f"{item.sequence.name}{topic.name}")

    return resources