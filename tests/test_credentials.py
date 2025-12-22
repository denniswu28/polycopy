from polycopy.config import Settings
from polycopy.credentials import ensure_api_credentials


class _StubCreds:
    def __init__(self, api_key: str = "derived-key", api_secret: str = "derived-secret", api_passphrase: str = "derived-pass") -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase


class _StubClient:
    def __init__(
        self,
        host: str,
        chain_id: int | None = None,
        key: str | None = None,
        signature_type: int | None = None,
        **_: object,
    ) -> None:
        self.host = host
        self.chain_id = chain_id
        self.key = key
        self.signature_type = signature_type
        self.nonce = None

    def create_or_derive_api_creds(self, *, nonce=None):
        self.nonce = nonce
        return _StubCreds()


def test_ensure_api_credentials_derives_when_missing():
    calls: list[_StubClient] = []

    def factory(**kwargs):
        client = _StubClient(**kwargs)
        calls.append(client)
        return client

    settings = Settings(private_key="0xabc", target_wallet="0xtarget", trader_wallet="0xme", signature_type=2)

    creds = ensure_api_credentials(settings, nonce=7, client_factory=factory)

    assert settings.api_key == "derived-key"
    assert settings.api_secret == "derived-secret"
    assert settings.api_passphrase == "derived-pass"
    assert creds.api_key == "derived-key"
    assert calls and calls[0].nonce == 7
    assert calls[0].signature_type == 2


def test_ensure_api_credentials_keeps_existing():
    def fail_factory(**kwargs):
        raise AssertionError(f"factory should not be called: {kwargs}")

    settings = Settings(
        private_key="0xabc",
        target_wallet="0xtarget",
        trader_wallet="0xme",
        api_key="existing-key",
        api_secret="existing-secret",
    )

    creds = ensure_api_credentials(settings, client_factory=fail_factory)

    assert creds.api_key == "existing-key"
    assert creds.api_secret == "existing-secret"
    assert creds.api_passphrase == ""
