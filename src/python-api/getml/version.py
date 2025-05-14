# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

import os
from pathlib import Path

DEFAULT_VERSION = "0.0.0"
VERSION_FILE = Path(__file__).parent / "VERSION"

try:
    __version__ = VERSION_FILE.read_text().strip()
except FileNotFoundError:
    __version__ = os.getenv("GETML_VERSION", DEFAULT_VERSION)
