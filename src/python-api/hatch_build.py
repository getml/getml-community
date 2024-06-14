import os
from hatchling.builders.hooks.plugin.interface import BuildHookInterface
from pathlib import Path
from wheel.bdist_wheel import get_platform

getml_bin_path = Path(__file__).parent / "getml" / ".getML"


class PlatformTag(BuildHookInterface):
    def initialize(self, version: str, build_data: dict) -> None:
        prefix = "py3-none-"
        if getml_bin_path.exists():
            if platform := os.getenv("WHEEL_PLATFORM"):
                build_data["tag"] = prefix + platform
            else:
                build_data["tag"] = prefix + get_platform(archive_root=None)
