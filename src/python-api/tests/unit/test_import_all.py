# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

from importlib import import_module
from pathlib import Path


def test_import_all():
    root = Path(__file__).parent.parent.parent / "getml"
    for path in root.rglob("*.py"):
        rel_path = path.relative_to(root)
        module = (
            str(rel_path).replace("/", ".").replace(".py", "").replace(".__init__", "")
        )
        import_module(f"getml.{module}")
