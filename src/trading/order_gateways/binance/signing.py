"""HMAC-SHA256 signing for Binance signed endpoints.

Pure module — no I/O, no network, no clock. Everything testable against
the worked examples in Binance's documentation.

Signing rules per Binance docs:

1. Concatenate query string and body, sorted as Binance presented them
   (in practice: query first, then form-encoded body if any).
2. HMAC-SHA256 with the API secret as key.
3. Hex digest as the ``signature`` parameter.
4. The ``X-MBX-APIKEY`` header carries the API key (not signed; just
   identifies the account).

Three subtleties worth knowing:

- Binance accepts the signature in either the query string or the form
  body; we always put it in the query for consistency.
- ``timestamp`` and ``recvWindow`` must be included in what gets signed,
  not appended after. The signer here takes a params dict; the caller is
  responsible for adding ``timestamp`` and (optionally) ``recvWindow``
  *before* passing it in.
- The order of parameters in the signed string must match the order in
  the actual request. We sort lexicographically and ensure the request
  uses the same order.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from urllib.parse import urlencode


def sign(params: Mapping[str, str | int | float], secret: str) -> str:
    """Compute the HMAC-SHA256 signature of ``params`` using ``secret``.

    Returns the hex digest. Does not mutate ``params``. The caller adds
    the result to its request as ``signature=<hex>``.
    """
    payload = urlencode(sorted(params.items()), doseq=False)
    return hmac.new(
        secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def encode_query(params: Mapping[str, str | int | float]) -> str:
    """URL-encode params in the same canonical order used by :func:`sign`.

    Keep this in lockstep with ``sign`` — if the encoding differs by one
    character, the signature will not match what Binance computes from
    the same request, and every signed call will return -1022.
    """
    return urlencode(sorted(params.items()), doseq=False)


__all__ = ["encode_query", "sign"]
