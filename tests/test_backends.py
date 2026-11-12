from __future__ import annotations

import pytest

from lakeback.backends import CsvBackend, ParquetBackend, get_backend
from lakeback.errors import BackendError, DataError, NotFoundError

SCHEMA = {"id": "int", "name": "str", "amount": "float", "active": "bool", "dt": "str"}
ROWS_A = [
    {"id": 2, "name": "beta", "amount": 12.5, "active": False, "dt": "2024-02-01"},
    {"id": 1, "name": "alpha", "amount": 7.25, "active": True, "dt": "2024-02-01"},
]
ROWS_B = [
    {"id": 10, "name": "gamma", "amount": 99.0, "active": True, "dt": "2024-02-02"},
]
BACKEND_NAMES = ["csv"] + (["parquet"] if ParquetBackend.available() else [])


@pytest.mark.parametrize("backend_name", BACKEND_NAMES)
def test_backend_roundtrip_scan_and_checksum(tmp_path, backend_name):
    backend = get_backend(backend_name)
    table = tmp_path / "table"
    backend.write_rows(table / "dt=2024-02-01" / f"part-00000.{backend.extension}", ROWS_A, SCHEMA)
    backend.write_rows(table / "dt=2024-02-02" / f"part-00000.{backend.extension}", ROWS_B, SCHEMA)

    files = backend.scan(table)
    assert [item.relative for item in files] == [
        "dt=2024-02-01/part-00000." + backend.extension,
        "dt=2024-02-02/part-00000." + backend.extension,
    ]
    assert all(len(backend.checksum(item.path)) == 64 for item in files)
    assert [item.partition for item in backend.scan(table, {"dt": "2024-02-02"})] == [
        {"dt": "2024-02-02"}
    ]
    rows = backend.read_rows(files[0].path, SCHEMA)
    assert rows == ROWS_A
    assert backend.infer_schema(files[0].path) == SCHEMA
    assert backend.read_table(table, SCHEMA) == ROWS_A + ROWS_B


def test_csv_and_parquet_are_logically_consistent(tmp_path):
    if not ParquetBackend.available():
        pytest.skip("pyarrow is not installed")
    csv_backend, parquet_backend = CsvBackend(), ParquetBackend()
    csv_root, parquet_root = tmp_path / "csv", tmp_path / "parquet"
    csv_backend.write_rows(csv_root / "dt=2024-02-01" / "p.csv", ROWS_A, SCHEMA)
    parquet_backend.write_rows(parquet_root / "dt=2024-02-01" / "p.parquet", ROWS_A, SCHEMA)
    assert csv_backend.read_table(csv_root, SCHEMA) == parquet_backend.read_table(
        parquet_root, SCHEMA
    )
    assert csv_backend.infer_schema(csv_root / "dt=2024-02-01" / "p.csv") == (
        parquet_backend.infer_schema(parquet_root / "dt=2024-02-01" / "p.parquet")
    )


@pytest.mark.parametrize("backend_name", BACKEND_NAMES)
def test_null_values_roundtrip(tmp_path, backend_name):
    backend = get_backend(backend_name)
    rows = [{"id": 1, "name": None, "amount": None, "active": None, "dt": "2024-02-01"}]
    path = tmp_path / "table" / "dt=2024-02-01" / f"part-00000.{backend.extension}"
    backend.write_rows(path, rows, SCHEMA)
    assert backend.read_rows(path, SCHEMA) == rows


@pytest.mark.parametrize("backend_name", BACKEND_NAMES)
def test_missing_table_directory(tmp_path, backend_name):
    with pytest.raises(NotFoundError):
        get_backend(backend_name).scan(tmp_path / "absent")


def test_csv_missing_schema_column_is_rejected(tmp_path):
    backend = CsvBackend()
    path = tmp_path / "part.csv"
    backend.write_rows(path, ROWS_A, SCHEMA)
    with pytest.raises(DataError, match="missing columns"):
        backend.read_rows(path, {"id": "int", "unknown": "str"})


def test_csv_empty_file_is_rejected(tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text("", encoding="utf-8")
    with pytest.raises(DataError, match="no header"):
        CsvBackend().read_rows(path)


def test_csv_bad_boolean_is_rejected(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("id,name,amount,active,dt\n1,a,1.0,maybe,2024-02-01\n", encoding="utf-8")
    with pytest.raises(DataError, match="boolean"):
        CsvBackend().read_rows(path, SCHEMA)


def test_unavailable_parquet_raises_backend_error(tmp_path, monkeypatch):
    monkeypatch.setattr("lakeback.backends.parquet_backend.PARQUET_AVAILABLE", False)
    backend = ParquetBackend()
    with pytest.raises(BackendError, match="unavailable"):
        backend.infer_schema(tmp_path / "anything.parquet")


def test_unknown_backend_rejected():
    with pytest.raises(ValueError, match="unknown backend"):
        get_backend("orc")
