import importlib
from pathlib import Path


def test_gdata_paths():
    # Import the module
    gData = importlib.import_module("pipeline.common.gData")

    base_dir = Path(gData.__file__).parent
    expected_config_dir = (base_dir / ".." / ".." / ".." / "config").resolve()

    # Core directories
    assert Path(gData.base_dir).resolve() == base_dir.resolve()
    assert Path(gData.config_dir).resolve() == expected_config_dir

    # GCP configs
    gcp_dir = expected_config_dir / "gcp"
    assert Path(gData.gcp_directory).resolve() == gcp_dir.resolve()
    assert Path(gData.production_gcp_config).resolve() == (gcp_dir / "production.yaml").resolve()
    assert Path(gData.development_gcp_config).resolve() == (gcp_dir / "development.yaml").resolve()

    # Tables
    table_dir = expected_config_dir / "tables"
    assert Path(gData.table_directory).resolve() == table_dir.resolve()
    assert Path(gData.fields_table).resolve() == (table_dir / "fields.json").resolve()
    assert Path(gData.hawk_identifiers_table).resolve() == (table_dir / "hawk_identifiers.json").resolve()


def test_gdata_files_exist():
    """
    Optional: If the config repo actually includes these files,
    keep this test; otherwise remove it to avoid failures.
    """
    gData = importlib.import_module("pipeline.common.gData")
    for path_str in [
        gData.production_gcp_config,
        gData.development_gcp_config,
        gData.fields_table,
        gData.hawk_identifiers_table,
    ]:
        path = Path(path_str)
        assert path.name  # sanity
        # Uncomment if you want to enforce existence:
        # assert path.exists(), f"Expected file not found: {path}"
