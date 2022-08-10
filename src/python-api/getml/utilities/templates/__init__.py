# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Manages jinja2 templates.
"""

from jinja2 import Environment, PackageLoader  # type: ignore

loader = PackageLoader("getml", "utilities/templates")
environment = Environment(loader=loader)
