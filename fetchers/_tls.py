"""Name a failed HTTPS request — first user: CONAB Layers 15/15b (#613).

``requests`` reports a TLS hostname mismatch as one long, nested exception
string that never says *which* certificate was served. That is the one fact
that tells a publisher's vhost misconfiguration apart from a DNS hijack, a
captive portal or an intercepting proxy. ``request_failure_reason`` reads
it:

* a hostname mismatch (OpenSSL verify code 62) triggers a second,
  handshake-only connection that reads the served certificate's names, then
  closes. That handshake still verifies the chain against the same trust store
  ``requests`` used (``CERT_REQUIRED``). Only the hostname comparison is
  skipped, because the names are what we want to read. No HTTP request is
  sent and no byte of application data is read, so nothing from an
  unverified peer can reach a table. The fetch itself stays a failure.
* the served name is attributed to the publisher's default vhost only when it
  *is* the name the caller says that vhost serves; any other name is "a
  certificate for another host".
* every other failure (unknown CA, timeout, refused, …) keeps the raw
  exception text.
"""

from __future__ import annotations

import os
import socket
import ssl
from collections.abc import Sequence
from urllib.parse import urlsplit

import requests
from requests.utils import DEFAULT_CA_BUNDLE_PATH

# OpenSSL's X509_V_ERR_HOSTNAME_MISMATCH: the chain is trusted but the
# certificate names another host.
X509_V_ERR_HOSTNAME_MISMATCH = 62


def _cert_verification_error(exc: BaseException) -> ssl.SSLCertVerificationError | None:
    """The ``ssl.SSLCertVerificationError`` behind a ``requests`` exception.

    requests wraps urllib3's ``MaxRetryError``, whose ``.reason`` wraps
    urllib3's ``SSLError``, whose first arg is the ssl error. Walk ``args``,
    ``reason``, ``__cause__`` and ``__context__`` until it turns up.
    """
    stack: list[object] = [exc]
    seen: set[int] = set()
    while stack:
        node = stack.pop()
        if id(node) in seen or not isinstance(node, BaseException):
            continue
        seen.add(id(node))
        if isinstance(node, ssl.SSLCertVerificationError):
            return node
        stack.extend(node.args)
        stack.extend((getattr(node, "reason", None), node.__cause__, node.__context__))
    return None


def _requests_trust_store(url: str) -> str:
    """The CA bundle ``requests.get(url)`` verifies against (env-aware)."""
    verify = requests.Session().merge_environment_settings(url, {}, None, None, None)["verify"]
    return verify if isinstance(verify, str) else DEFAULT_CA_BUNDLE_PATH


def served_certificate_names(url: str, timeout: float) -> list[str]:
    """DNS names (SAN, else CN) of the certificate the server at ``url`` presents.

    Handshake only, over a direct connection with the same SNI ``requests``
    sends. The chain is verified; only the hostname check is skipped. Raises
    ``OSError``/``ssl.SSLError`` if the handshake itself fails.
    """
    parts = urlsplit(url)
    host = parts.hostname or ""
    trust = _requests_trust_store(url)
    ctx = (
        ssl.create_default_context(capath=trust)
        if os.path.isdir(trust)
        else ssl.create_default_context(cafile=trust)
    )
    ctx.check_hostname = False  # reading the name is the point; chain stays CERT_REQUIRED
    with (
        socket.create_connection((host, parts.port or 443), timeout=timeout) as raw,
        ctx.wrap_socket(raw, server_hostname=host) as tls,
    ):
        cert = tls.getpeercert()
    if not cert:
        return []
    san: tuple[tuple[str, str], ...] = cert.get("subjectAltName", ())  # type: ignore[assignment]
    names = [value for kind, value in san if kind == "DNS"]
    if names:
        return names
    subject: tuple[tuple[tuple[str, str], ...], ...] = cert.get("subject", ())  # type: ignore[assignment]
    return [value for rdn in subject for key, value in rdn if key == "commonName"]


def request_failure_reason(
    exc: requests.RequestException,
    url: str,
    *,
    publisher: str,
    default_vhost_name: str | None = None,
    timeout: float = 30,
) -> str:
    """One line naming why ``requests.get(url)`` failed.

    ``default_vhost_name`` is the name the publisher's front end serves when
    no vhost matches (the cert a no-SNI client gets). A mismatch is
    attributed to that vhost only when that is the name actually served.
    """
    cert_error = _cert_verification_error(exc)
    if cert_error is None or cert_error.verify_code != X509_V_ERR_HOSTNAME_MISMATCH:
        return str(exc)

    host = urlsplit(url).hostname
    try:
        names: Sequence[str] = served_certificate_names(url, timeout)
    except (OSError, ssl.SSLError, ValueError) as probe_exc:
        served = f"a certificate for another host (names unreadable: {probe_exc})"
    else:
        if default_vhost_name is not None and list(names) == [default_vhost_name]:
            served = f"its default-vhost certificate ({default_vhost_name})"
        else:
            served = f"a certificate for another host ({', '.join(names) or 'no DNS name'})"
    return (
        f"TLS hostname mismatch: {publisher} presented {served} when asked "
        f"for {host!r}; verification stays on"
    )


__all__: Sequence[str] = (
    "X509_V_ERR_HOSTNAME_MISMATCH",
    "request_failure_reason",
    "served_certificate_names",
)
