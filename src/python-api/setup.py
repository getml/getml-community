# copyright 2022 the sqlnet company gmbh
#
# this file is licensed under the elastic license 2.0 (elv2).
# refer to the license.txt file in the root of the repository
# for details.
#


from platform import machine
from sys import argv

from setuptools import setup


if argv[1] == "bdist_wheel":
    argv.extend(["--plat-name", f"manylinux_2_17_{machine()}"])


setup()
