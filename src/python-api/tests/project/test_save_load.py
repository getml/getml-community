import os
import zipfile
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest

import getml


@pytest.fixture
def bundle(df, tmpdir):
    df.save()
    bundle = Path(f"{getml.project.name}.getml")
    getml.project.save(bundle)
    getml.project.delete()
    yield bundle


def test_save_project_bundle(engine, bundle, df):
    assert bundle.exists()
    assert bundle.stat().st_size > 0
    assert bundle.suffix == ".getml"
    assert zipfile.is_zipfile(bundle)

    with zipfile.ZipFile(bundle, "r") as zf:
        zf.extractall()
        output_dir = Path(bundle.stem)
        assert output_dir.exists()
        extracted_files = output_dir.rglob("*")
        assert len(list(extracted_files)) > 0
        assert (output_dir / f"data/{df.name}").exists()


def test_save_project_bundle_default_name(engine, df, tmpdir):
    getml.project.save()
    assert Path(f"{getml.project.name}.getml").exists()


def test_load_project(engine, bundle, df):
    getml.project.load(bundle)
    getml.project.data_frames.load()
    assert df.name in [df.name for df in getml.project.data_frames.data]
    assert getml.project.name == bundle.stem
    assert [df.name for df in getml.project.data_frames.data]
