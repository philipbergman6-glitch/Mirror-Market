"""CONAB TLS hostname-mismatch replay (#613).

The failure (deploy runs 2026-10-22 → 11-06 and 2026-11-17 → 11-25):

    SSLCertVerificationError(1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate
    verify failed: Hostname mismatch, certificate is not valid for
    'portaldeinformacoes.conab.gov.br'. (_ssl.c:1011)")

CONAB's front end (Apache 2.4.71, AS267310) hosts several name-based vhosts on
one IP. When the portal vhost is not answering, Apache hands out its *default*
vhost certificate — ``CN=www.conab.gov.br`` (the cert it presents to a
client that sends no SNI, probed 2026-11-25) — for every name. This module
replays that through the real ``requests`` → ``urllib3`` → ``ssl`` stack: a
loopback TLS server presents a CA-signed certificate valid only for
``www.conab.gov.br`` while the fetcher asks for ``localhost``. The positive
control (same server, a certificate valid for ``localhost``) proves the harness
is red for the mismatch alone, not for some other handshake fault.

What is pinned: the layer still fails (verification is never relaxed — no
call passes ``verify=False``), and the failure is *named*, so the deploy log
says "CONAB's server presented a certificate for another host" instead of
"returned no data".
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

from fetchers import conab, conab_precos

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

    def do_GET(self) -> None:  # noqa: N802 — http.server API
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(self.body)))
        self.send_header("Last-Modified", _LAST_MODIFIED)
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass


@pytest.fixture()
def conab_portal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Start a loopback CONAB portal presenting a certificate for ``cert_name``.

    Returns ``serve(cert_name, body) -> base_url``. The test CA is trusted via
    ``REQUESTS_CA_BUNDLE`` (what ``requests.get`` honours), so a certificate
    for the right name verifies and one for the wrong name fails on the
    hostname check alone.
    """
    ca_key, ca_cert = _make_ca()
    ca_path = tmp_path / "ca.pem"
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(ca_path))
    monkeypatch.setattr(conab, "retry_sleep", lambda attempt: None)
    monkeypatch.setattr(conab_precos, "retry_sleep", lambda attempt: None)

    servers: list[http.server.HTTPServer] = []

    def serve(cert_name: str, body: bytes) -> str:
        key, cert = _make_leaf(ca_key, ca_cert, cert_name)
        cert_path = tmp_path / f"{cert_name}.pem"
        key_path = tmp_path / f"{cert_name}.key"
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
        handler = type("H", (_Handler,), {"body": body})
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"https://localhost:{server.server_address[1]}"

    yield serve
    for server in servers:
        server.shutdown()
        server.server_close()


@pytest.fixture()
def get_calls(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[dict[str, object]]]:
    """Record every ``requests.get`` kwargs set both fetchers send."""
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


# ── Layer 15 ───────────────────────────────────────────────────────────────

def test_layer15_positive_control_correct_vhost_cert_fetches(
    conab_portal, get_calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = conab_portal("localhost", _serie_historica())
    monkeypatch.setattr(conab, "CONAB_URL", f"{base}/downloads/arquivos/SerieHistoricaGraos.txt")
    df = conab.fetch_conab_estimates()
    assert not df.empty
    _assert_verification_never_relaxed(get_calls)


def test_layer15_default_vhost_cert_fails_named_and_verified(
    conab_portal, get_calls, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    base = conab_portal("www.conab.gov.br", _serie_historica())
    monkeypatch.setattr(conab, "CONAB_URL", f"{base}/downloads/arquivos/SerieHistoricaGraos.txt")
    with caplog.at_level("WARNING", logger="fetchers.conab"):
        df = conab.fetch_conab_estimates()

    assert df.empty  # the layer still fails — no row from an unverified peer
    _assert_verification_never_relaxed(get_calls)
    assert len(get_calls) == conab.MAX_RETRIES
    final = [r for r in caplog.records if r.levelname == "ERROR"]
    assert final, "no ERROR line for the exhausted retries"
    assert "TLS hostname mismatch" in final[-1].getMessage()
    assert "localhost" in final[-1].getMessage()


# ── Layer 15b ──────────────────────────────────────────────────────────────

def test_layer15b_positive_control_correct_vhost_cert_fetches(
    conab_portal, get_calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = conab_portal("localhost", _precos())
    monkeypatch.setattr(
        conab_precos, "CONAB_PRECOS_URL", f"{base}/downloads/arquivos/PrecosSemanalUF.txt"
    )
    result = conab_precos.fetch_conab_farmgate()
    assert result.status == "ok", result.error
    _assert_verification_never_relaxed(get_calls)


def test_layer15b_default_vhost_cert_fails_named_and_verified(
    conab_portal, get_calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = conab_portal("www.conab.gov.br", b"unused")
    monkeypatch.setattr(
        conab_precos, "CONAB_PRECOS_URL", f"{base}/downloads/arquivos/PrecosSemanalUF.txt"
    )
    result = conab_precos.fetch_conab_farmgate()

    assert result.status == "failed"
    _assert_verification_never_relaxed(get_calls)
    assert len(get_calls) == conab_precos.MAX_RETRIES
    assert result.error is not None
    assert "TLS hostname mismatch" in result.error
    assert "localhost" in result.error
