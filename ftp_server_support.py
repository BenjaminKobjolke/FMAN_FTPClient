"""Shared local FTP/FTPS server for offline tests and the CLI driver."""
from datetime import datetime, timedelta, timezone
import ipaddress
import logging
import os
import shutil
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
TEST_VENDOR = os.path.join(HERE, 'test-vendor')
sys.path.insert(0, TEST_VENDOR)

logging.getLogger('pyftpdlib').setLevel(logging.CRITICAL)

FTP_TEST_USER = 'user'
FTP_TEST_PASSWORD = 'secret'


def _write_certificate(directory: str) -> str:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, '127.0.0.1'),
    ])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([
            x509.IPAddress(ipaddress.ip_address('127.0.0.1')),
        ]), critical=False)
        .sign(key, hashes.SHA256())
    )
    certificate_path = os.path.join(directory, 'localhost.pem')
    with open(certificate_path, 'wb') as stream:
        stream.write(certificate.public_bytes(serialization.Encoding.PEM))
        stream.write(key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ))
    return certificate_path


class FtpTestServer:
    """Run a password-protected FTP server on a local ephemeral port."""

    def __init__(
        self, tls: bool = False, root: str | None = None,
        delay_seconds: float = 0.0,
    ) -> None:
        if delay_seconds < 0:
            raise ValueError('delay_seconds must be non-negative')
        from pyftpdlib.authorizers import DummyAuthorizer
        from pyftpdlib.handlers import FTPHandler, TLS_FTPHandler
        from pyftpdlib.servers import FTPServer, ThreadedFTPServer

        self._owns_root = root is None
        self.root = root or tempfile.mkdtemp(prefix='ftpclient-ftp-')
        self.delay_seconds = delay_seconds
        self.cert_root = tempfile.mkdtemp(prefix='ftpclient-ftps-') if tls else None
        authorizer = DummyAuthorizer()
        authorizer.add_user(
            FTP_TEST_USER, FTP_TEST_PASSWORD, self.root, perm='elradfmwMT')

        if tls:
            handler_type = TLS_FTPHandler
            attributes = {
                'tls_control_required': True,
                'tls_data_required': True,
                'certfile': _write_certificate(self.cert_root),
                'ssl_context': None,
            }
        else:
            handler_type = FTPHandler
            attributes = {}
        server_type = FTPServer
        if delay_seconds > 0:
            def delayed(operation):
                def run(handler, *args, **kwargs):
                    # Keep fman's progress window visible long enough to inspect it.
                    time.sleep(delay_seconds)
                    return operation(handler, *args, **kwargs)
                return run

            attributes.update({
                name: delayed(getattr(handler_type, name))
                for name in ('ftp_RETR', 'ftp_STOR', 'ftp_DELE',
                             'ftp_RMD', 'ftp_MKD')
            })
            server_type = ThreadedFTPServer
        handler = type('LocalFtpHandler', (handler_type,), attributes)
        handler.authorizer = authorizer

        self.server = server_type(('127.0.0.1', 0), handler)
        self.port = self.server.socket.getsockname()[1]
        self.running = True
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        while self.running:
            self.server.serve_forever(
                timeout=0.05, blocking=False, handle_exit=False)
        self.server.close_all()

    def close(self) -> None:
        self.running = False
        self.thread.join(timeout=2)
        if self._owns_root:
            shutil.rmtree(self.root, ignore_errors=True)
        if self.cert_root is not None:
            shutil.rmtree(self.cert_root, ignore_errors=True)
