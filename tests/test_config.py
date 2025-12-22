import pytest

from polycopy.config import PROJECT_ROOT, Settings, load_settings


def test_load_settings_surfaces_missing_required(monkeypatch, capsys):
    required_fields: list[str] = []
    for name, field in Settings.model_fields.items():
        if callable(getattr(field, "is_required", None)) and field.is_required():
            required_fields.append(name)
    for env_var in (field.upper() for field in required_fields):
        monkeypatch.delenv(env_var, raising=False)

    with pytest.raises(SystemExit):
        load_settings([])

    err = capsys.readouterr().err
    assert "Missing required settings" in err
    for field in required_fields:
        assert field in err


def test_load_settings_reads_project_env_when_cwd_differs(monkeypatch, tmp_path):
    env_path = PROJECT_ROOT / ".env"
    original_contents = env_path.read_text() if env_path.exists() else None
    env_path.write_text("PRIVATE_KEY=fromenv\nTARGET_WALLET=0xAA\nTRADER_WALLET=0xBB\n")

    for var in ("PRIVATE_KEY", "TARGET_WALLET", "TRADER_WALLET"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.chdir(tmp_path)

    try:
        settings, _ = load_settings([])
        assert settings.private_key == "fromenv"
        assert settings.target_wallet == "0xAA"
        assert settings.trader_wallet == "0xBB"
    finally:
        if original_contents is None:
            env_path.unlink(missing_ok=True)
        else:
            env_path.write_text(original_contents)
