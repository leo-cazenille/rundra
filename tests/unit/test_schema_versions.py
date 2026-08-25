import pytest

from rundra.schema_versions import PUBLIC_SCHEMA_VERSIONS, SchemaVersions


def test_public_schema_registry_is_internally_consistent() -> None:
    assert PUBLIC_SCHEMA_VERSIONS
    assert all(
        schema.current in schema.supported for schema in PUBLIC_SCHEMA_VERSIONS.values()
    )
    assert PUBLIC_SCHEMA_VERSIONS["run_record"].current == 7
    assert PUBLIC_SCHEMA_VERSIONS["plan"].current == 9
    assert PUBLIC_SCHEMA_VERSIONS["targets_config"].current == 11


def test_schema_versions_rejects_an_unsupported_current_version() -> None:
    with pytest.raises(ValueError, match="must be supported"):
        SchemaVersions(3, frozenset({1, 2}))
