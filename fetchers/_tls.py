"""Name a failed HTTPS request — first user: CONAB Layers 15/15b (#613).

``requests`` reports a TLS hostname mismatch as one long, nested exception
string that never says *which* certificate was served. That is the one fact
that tells a publisher's vhost misconfiguration apart from a DNS hijack, a
captive portal or an intercepting proxy. Python's ``ssl`` does not expose the
peer certificate of a handshake that failed verification, and capturing it
from the fetch connection would mean loosening the fetch's own verification.
That is never acceptable, so the failing certificate itself is not available.
``request_failure_reason`` therefore states only what it knows:

* the **fact** of the failed attempt: a certificate not valid for the
  requested host (OpenSSL verify code 62);
* then, labelled as such, what a **re-read** saw. That is a separate,
  handshake-only connection made once per failed run. It sends the same SNI,
  verifies the chain against the trust store ``requests`` used
  (``CERT_REQUIRED``) and skips only the hostname comparison, because the
  names are what it reads. It sends no HTTP request and reads no application
  data, so nothing from an unverified peer can reach a table, and the fetch
  stays a failure. The fault is intermittent, so the re-read may differ from
  the failing attempt:

  * a re-read whose certificate *is* valid for the host says so, "the
    mismatch is intermittent (the failing certificate was not captured)",
    and attributes it to no one;
  * the publisher's default vhost is named only when the re-read is served
    exactly ``default_vhost_name``; any other name is "a certificate for
    another host";
  * when ``requests`` would reach the URL through a proxy (``HTTPS_PROXY``
    / ``ALL_PROXY``, ``NO_PROXY`` honoured), a direct re-read would take a
    different route and could describe a different server, so it is not
    attempted.

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


def _proxied(url: str) -> bool:
    """Whether ``requests.get(url)`` would go through a proxy (env-aware)."""
    proxies = requests.utils.get_environ_proxies(url)
    return bool(proxies.get(urlsplit(url).scheme) or proxies.get("all"))


def _valid_for(host: str, names: Sequence[str]) -> bool:
    """RFC 6125-style: exact name, or a ``*.`` wildcard covering one left label."""
    host = host.lower().rstrip(".")
    for name in (n.lower().rstrip(".") for n in names):
        if name == host:
            return True
        if name.startswith("*.") and "." in host and host.split(".", 1)[1] == name[2:]:
            return True
    return False


def request_failure_reason(
    exc: requests.RequestException,
    url: str,
    *,
    publisher: str,
    timeout: float,
    default_vhost_name: str | None = None,
) -> str:
    """One line naming why ``requests.get(url)`` failed.

    Call it once per failed run, on the last attempt's exception: a mismatch
    costs one extra handshake, bounded by ``timeout``. ``default_vhost_name``
    is the name the publisher's front end serves when no vhost matches (the
    certificate a no-SNI client gets).
    """
    cert_error = _cert_verification_error(exc)
    if cert_error is None or cert_error.verify_code != X509_V_ERR_HOSTNAME_MISMATCH:
        return str(exc)

    host = urlsplit(url).hostname or ""
    failed = (
        f"TLS hostname mismatch: {publisher} presented a certificate not valid "
        f"for {host!r} (verification stays on)"
    )
    if _proxied(url):
        return (
            f"{failed}; the served certificate was not re-read, because requests "
            "routes this URL via a proxy and a direct handshake would take another route"
        )
    try:
        names: Sequence[str] = served_certificate_names(url, timeout)
    except (OSError, ssl.SSLError, ValueError) as probe_exc:
        return f"{failed}; the served certificate could not be re-read ({probe_exc})"
    if _valid_for(host, names):
        return (
            f"{failed}; a re-read handshake got a valid certificate, so the mismatch "
            "is intermittent (the failing certificate was not captured)"
        )
    if default_vhost_name is not None and list(names) == [default_vhost_name]:
        return f"{failed}; a re-read handshake was served its default-vhost certificate ({default_vhost_name})"
    return (
        f"{failed}; a re-read handshake was served a certificate for another host "
        f"({', '.join(names) or 'no DNS name'})"
    )


__all__: Sequence[str] = (
    "X509_V_ERR_HOSTNAME_MISMATCH",
    "request_failure_reason",
    "served_certificate_names",
)
