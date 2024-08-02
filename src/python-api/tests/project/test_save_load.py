import os
import zipfile
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest

import getml


@pytest.fixture
def bundle(getml_project, df, tmpdir):
    df.save()
    bundle = Path(f"{getml_project.name}.getml")
    getml_project.save(bundle)
    getml_project.delete()
    yield bundle


def test_save_project_bundle(getml_project, bundle, df):
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


def test_save_project_bundle_default_name(getml_project, df, tmpdir):
    getml_project.save()
    assert Path(f"{getml_project.name}.getml").exists()


def test_load_project(getml_project, bundle, df):
    getml_project.load(bundle)
    getml_project.data_frames.load()
    assert df.name in [df.name for df in getml_project.data_frames.data]
    assert getml_project.name == bundle.stem
    assert [df.name for df in getml_project.data_frames.data]
