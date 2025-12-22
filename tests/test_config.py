import pytest

from polycopy.config import load_settings


def test_load_settings_surfaces_missing_required(monkeypatch, capsys):
    for env in ("PRIVATE_KEY", "TARGET_WALLET", "TRADER_WALLET"):
        monkeypatch.delenv(env, raising=False)

    with pytest.raises(SystemExit):
        load_settings([])

    err = capsys.readouterr().err
    assert "Missing required settings" in err
    for field in ("private_key", "target_wallet", "trader_wallet"):
        assert field in err
