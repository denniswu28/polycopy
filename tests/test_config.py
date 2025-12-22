import pytest
from pydantic_core import PydanticUndefined

from polycopy.config import Settings, load_settings


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
