"""Custom hatchling build hook for sqlitegis.

The wheel ships a Rust cdylib under ``sqlitegis/_bin/`` (placed there by
``bindings/python/scripts/before_build.sh`` in the cibuildwheel matrix).
The cdylib does not bind to CPython's C API; it is loaded at runtime by
``sqlite3.Connection.load_extension``. So the wheel is platform-specific
but not Python-version-specific.

This hook sets the wheel tag to ``py3-none-<platform>`` explicitly so one
wheel works on every CPython 3.x for that platform, instead of the
``cpXX-cpXX-<platform>`` tag hatchling's ``infer_tag`` would produce
(which would force us to build one wheel per Python minor version).

The default ``py3-none-any`` wheel hatchling would otherwise generate
gets rejected by cibuildwheel for binary-carrier projects.
"""

from __future__ import annotations

import sysconfig
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    """Tag the wheel as ``py3-none-<platform>``."""

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        # `sysconfig.get_platform()` returns e.g. 'linux-x86_64',
        # 'macosx-11.0-arm64', 'win-amd64'. PEP 427 wheel tags use
        # underscores in place of '-' and '.', so normalise.
        platform_tag = sysconfig.get_platform().replace("-", "_").replace(".", "_")
        build_data["tag"] = f"py3-none-{platform_tag}"
        build_data["pure_python"] = False
