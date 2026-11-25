"""CONAB TLS hostname-mismatch replay (#613).

The failure (deploy runs 2026-10-22 → 11-06 and 2026-11-17 → 11-25):

    SSLCertVerificationError(1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate
    verify failed: Hostname mismatch, certificate is not valid for
    'portaldeinformacoes.conab.gov.br'. (_ssl.c:1011)")

CONAB's front end (Apache 2.4.71, AS267310) hosts several name-based vhosts on
one IP, and presents ``CN=www.conab.gov.br`` — its default vhost — to a client
that sends no SNI (probed 2026-11-25). The working hypothesis is that the
failing runs were handed that default certificate for the portal name; the
deploy log does not record which certificate was served, which is why the
classifier now reads it. This module replays the failure through the real
``requests`` → ``urllib3`` → ``ssl`` stack: a loopback TLS server presents a
CA-signed certificate for one name while the fetcher asks for ``localhost``.
The positive control (a certificate valid for ``localhost``) proves the
harness is red for the mismatch alone.

What is pinned:

* the layer still fails — verification is never relaxed, no call passes
  ``verify=False``;
* the failure is *named*, and names CONAB's default vhost only when the
  served certificate is in fact ``www.conab.gov.br``; any other name is
  "a certificate for another host" (a DNS hijack, captive portal or
  intercepting proxy is not CONAB's fault);
* a different verification failure (unknown CA) keeps its raw text;
* the certificate-name probe completes a handshake only — no HTTP request
  reaches the server.
"""

from __future__ import annotations

import datetime as dt
import gzip
import http.server
import ssl
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from fetchers import _tls, conab, conab_precos

FIXTURES = Path(__file__).parent / "fixtures"
_LAST_MODIFIED = "Tue, 11 Aug 2026 11:00:16 GMT"  # conab_serie_historica_graos.headers.txt


# ── a throwaway CA and leaf certificates ───────────────────────────────────

def _name(cn: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def _make_ca() -> tuple[ec.EllipticCurvePrivateKey, x509.Certificate]:
    key = ec.generate_private_key(ec.SECP256R1())
    now = dt.datetime.now(dt.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(_name("Mirror Market test CA"))
        .issuer_name(_name("Mirror Market test CA"))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, key_cert_sign=True, crl_sign=True,
                content_commitment=False, key_encipherment=False,
                data_encipherment=False, key_agreement=False,
                encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )
    return key, cert


def _make_leaf(
    ca_key: ec.EllipticCurvePrivateKey, ca_cert: x509.Certificate, dns_name: str
) -> tuple[ec.EllipticCurvePrivateKey, x509.Certificate]:
    key = ec.generate_private_key(ec.SECP256R1())
    now = dt.datetime.now(dt.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(_name(dns_name))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(dns_name)]), critical=False)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    return key, cert


# ── loopback HTTPS server ──────────────────────────────────────────────────

class _Handler(http.server.BaseHTTPRequestHandler):
    body: bytes = b""
    hits: list[str]

    def do_GET(self) -> None:
        self.hits.append(self.path)
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(self.body)))
        self.send_header("Last-Modified", _LAST_MODIFIED)
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, format: str, *args: object) -> None:
        pass


class _Portal:
    def __init__(self, base: str, hits: list[str], handshakes: list[str | None]) -> None:
        self.base = base
        self.hits = hits
        self.handshakes = handshakes  # SNI of every ClientHello, in order


@pytest.fixture()
def conab_portal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Start a loopback CONAB portal presenting a certificate for ``cert_name``.

    Returns ``serve(cert_name, body, trusted=True) -> _Portal``. The test CA
    is trusted via ``REQUESTS_CA_BUNDLE`` (what ``requests.get`` honours), so
    a certificate for the right name verifies and one for the wrong name
    fails on the hostname check alone. ``trusted=False`` signs with a second
    CA nobody trusts (OpenSSL verify code 20).
    """
    ca_key, ca_cert = _make_ca()
    rogue_key, rogue_cert = _make_ca()
    ca_path = tmp_path / "ca.pem"
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(ca_path))
    monkeypatch.setattr(conab, "retry_sleep", lambda attempt: None)
    monkeypatch.setattr(conab_precos, "retry_sleep", lambda attempt: None)

    servers: list[http.server.HTTPServer] = []

    def server_context(cert_name: str, trusted: bool) -> ssl.SSLContext:
        signer_key, signer_cert = (ca_key, ca_cert) if trusted else (rogue_key, rogue_cert)
        key, cert = _make_leaf(signer_key, signer_cert, cert_name)
        stem = f"{cert_name}-{'trusted' if trusted else 'rogue'}"
        cert_path = tmp_path / f"{stem}.pem"
        key_path = tmp_path / f"{stem}.key"
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert_path, key_path)
        return ctx

    def serve(
        cert_name: str,
        body: bytes,
        trusted: bool = True,
        *,
        then_name: str | None = None,
        flip_after: int = 0,
    ) -> _Portal:
        """``then_name``: after ``flip_after`` handshakes, present that cert instead.

        Models the intermittent fault: the fetch attempts get the wrong
        certificate and a later handshake gets the right one.
        """
        ctx = server_context(cert_name, trusted)
        later = server_context(then_name, trusted) if then_name else None
        handshakes: list[str | None] = []

        def on_hello(sslobj: ssl.SSLObject, server_name: str | None, _ctx: ssl.SSLContext) -> None:
            handshakes.append(server_name)
            if later is not None and len(handshakes) > flip_after:
                sslobj.context = later

        ctx.sni_callback = on_hello  # type: ignore[assignment]
        hits: list[str] = []
        handler = type("H", (_Handler,), {"body": body, "hits": hits})
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return _Portal(f"https://localhost:{server.server_address[1]}", hits, handshakes)

    yield serve
    for server in servers:
        server.shutdown()
        server.server_close()


@pytest.fixture()
def get_calls(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[dict[str, object]]]:
    """Record the kwargs of every ``requests.get`` both fetchers send."""
    calls: list[dict[str, object]] = []
    real_get = requests.get

    def spy(url: str, **kwargs: object) -> requests.Response:
        calls.append(kwargs)
        return real_get(url, **kwargs)

    monkeypatch.setattr(conab.requests, "get", spy)
    monkeypatch.setattr(conab_precos.requests, "get", spy)
    yield calls


def _serie_historica() -> bytes:
    return gzip.decompress((FIXTURES / "conab_serie_historica_graos.txt.gz").read_bytes())


def _precos() -> bytes:
    return gzip.decompress((FIXTURES / "conab_precos_semanal_uf.txt.gz").read_bytes())


def _assert_verification_never_relaxed(calls: list[dict[str, object]]) -> None:
    assert calls, "the fetcher never reached requests.get"
    assert all(c.get("verify", True) is not False for c in calls), calls


def _point_layer15(monkeypatch: pytest.MonkeyPatch, portal: _Portal) -> None:
    monkeypatch.setattr(conab, "CONAB_URL", f"{portal.base}/downloads/arquivos/SerieHistoricaGraos.txt")


def _point_layer15b(monkeypatch: pytest.MonkeyPatch, portal: _Portal) -> None:
    monkeypatch.setattr(conab_precos, "CONAB_PRECOS_URL", f"{portal.base}/downloads/arquivos/PrecosSemanalUF.txt")


# ── Layer 15 ───────────────────────────────────────────────────────────────

def test_layer15_positive_control_correct_vhost_cert_fetches(
    conab_portal, get_calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    portal = conab_portal("localhost", _serie_historica())
    _point_layer15(monkeypatch, portal)
    result = conab.fetch_conab_estimates()
    assert result.status == "ok", result.error
    assert not result.data["national"].empty
    _assert_verification_never_relaxed(get_calls)


def test_layer15_default_vhost_cert_fails_named_and_verified(
    conab_portal, get_calls, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    portal = conab_portal("www.conab.gov.br", _serie_historica())
    _point_layer15(monkeypatch, portal)
    with caplog.at_level("WARNING", logger="fetchers.conab"):
        result = conab.fetch_conab_estimates()

    assert result.status == "failed"  # no row from an unverified peer
    _assert_verification_never_relaxed(get_calls)
    assert len(get_calls) == conab.MAX_RETRIES
    assert portal.hits == [], "a request reached the server despite the failed handshake"
    # one handshake per fetch attempt, plus ONE certificate re-read per run
    assert len(portal.handshakes) == conab.MAX_RETRIES + 1, portal.handshakes
    assert result.error is not None
    assert "TLS hostname mismatch" in result.error
    assert "default-vhost certificate (www.conab.gov.br)" in result.error
    assert "'localhost'" in result.error
    final = [r for r in caplog.records if r.levelname == "ERROR"]
    assert final and result.error in final[-1].getMessage()


# ── Layer 15b ──────────────────────────────────────────────────────────────

def test_layer15b_positive_control_correct_vhost_cert_fetches(
    conab_portal, get_calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    portal = conab_portal("localhost", _precos())
    _point_layer15b(monkeypatch, portal)
    result = conab_precos.fetch_conab_farmgate()
    assert result.status == "ok", result.error
    _assert_verification_never_relaxed(get_calls)


def test_layer15b_default_vhost_cert_fails_named_and_verified(
    conab_portal, get_calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    portal = conab_portal("www.conab.gov.br", b"unused")
    _point_layer15b(monkeypatch, portal)
    result = conab_precos.fetch_conab_farmgate()

    assert result.status == "failed"
    _assert_verification_never_relaxed(get_calls)
    assert len(get_calls) == conab_precos.MAX_RETRIES
    assert portal.hits == []
    assert len(portal.handshakes) == conab_precos.MAX_RETRIES + 1, portal.handshakes
    assert result.error is not None
    assert "TLS hostname mismatch" in result.error
    assert "default-vhost certificate (www.conab.gov.br)" in result.error
    assert "'localhost'" in result.error


# ── negatives: the classifier must not blame CONAB for what it can't see ──

def test_unrelated_host_cert_is_another_host_not_default_vhost(
    conab_portal, get_calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A DNS hijack / captive portal / intercepting proxy is not CONAB's vhost."""
    portal = conab_portal("evil.example", b"unused")
    _point_layer15b(monkeypatch, portal)
    result = conab_precos.fetch_conab_farmgate()

    assert result.status == "failed"
    _assert_verification_never_relaxed(get_calls)
    assert portal.hits == []
    assert result.error is not None
    assert "TLS hostname mismatch" in result.error
    assert "a certificate for another host (evil.example)" in result.error
    assert "default-vhost" not in result.error


def test_unknown_ca_keeps_raw_verification_text(
    conab_portal, get_calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify code 20 (untrusted issuer) is not a hostname mismatch."""
    portal = conab_portal("localhost", b"unused", trusted=False)
    _point_layer15(monkeypatch, portal)
    result = conab.fetch_conab_estimates()

    assert result.status == "failed"
    _assert_verification_never_relaxed(get_calls)
    assert portal.hits == []
    assert result.error is not None
    assert "hostname mismatch" not in result.error.lower()
    assert "CERTIFICATE_VERIFY_FAILED" in result.error
    assert "unable to get local issuer certificate" in result.error


def test_reread_that_gets_a_valid_cert_never_contradicts_the_failure(
    conab_portal, get_calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wave-2 review: every fetch attempt is served the default-vhost cert,
    the re-read handshake afterwards gets the right one. The reason must not
    claim "a certificate for another host (localhost)" for 'localhost'."""
    portal = conab_portal(
        "www.conab.gov.br", b"unused", then_name="localhost", flip_after=conab_precos.MAX_RETRIES
    )
    _point_layer15b(monkeypatch, portal)
    result = conab_precos.fetch_conab_farmgate()

    assert result.status == "failed"
    _assert_verification_never_relaxed(get_calls)
    assert portal.hits == []
    assert len(portal.handshakes) == conab_precos.MAX_RETRIES + 1
    assert result.error is not None
    assert "TLS hostname mismatch" in result.error
    assert "another host (localhost)" not in result.error
    assert "default-vhost" not in result.error
    assert "intermittent" in result.error
    assert "not captured" in result.error


def test_proxied_route_is_not_reread(
    conab_portal, get_calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    """requests honours HTTPS_PROXY; a direct re-read would take another
    route and could describe a different server, so it is not attempted."""
    portal = conab_portal("www.conab.gov.br", b"unused")
    _point_layer15b(monkeypatch, portal)
    monkeypatch.setattr(_tls, "_proxied", lambda url: True)  # requests would route via a proxy
    result = conab_precos.fetch_conab_farmgate()

    assert result.status == "failed"
    assert len(portal.handshakes) == conab_precos.MAX_RETRIES  # no re-read
    assert result.error is not None
    assert "TLS hostname mismatch" in result.error
    assert "proxy" in result.error
    assert "default-vhost" not in result.error


@pytest.mark.parametrize(
    ("host", "names", "valid"),
    [
        ("portaldeinformacoes.conab.gov.br", ["portaldeinformacoes.conab.gov.br"], True),
        ("portaldeinformacoes.conab.gov.br", ["PortalDeInformacoes.conab.gov.br."], True),
        ("portaldeinformacoes.conab.gov.br", ["*.conab.gov.br"], True),
        ("portaldeinformacoes.conab.gov.br", ["www.conab.gov.br"], False),
        ("a.b.conab.gov.br", ["*.conab.gov.br"], False),  # a wildcard covers one label only
        ("conab.gov.br", ["*.conab.gov.br"], False),
        ("portaldeinformacoes.conab.gov.br", [], False),
    ],
)
def test_valid_for_matches_like_a_tls_client(host: str, names: list[str], valid: bool) -> None:
    assert _tls._valid_for(host, names) is valid
