# Copyright 2022 The SQLNet Company GmbH
# 
# This file is licensed under the Elastic License 2.0 (ELv2). 
# Refer to the LICENSE.txt file in the root of the repository 
# for details.
# 

"""setup.py for getml"""

from setuptools import find_packages, setup

exec(open("getml/version.py").read())

long_description = """
    getML (https://getml.com) is a software for automated machine learning
    (AutoML) with a special focus on feature learning for relational data
    and time series.

    This is the official python client for the getML engine.

    Documentation and more details at https://docs.getml.com/{}
""".format(
    __version__
)


setup(
    name="getml",
    version=__version__,
    author="getML",
    author_email="support@getml.com",
    url="https://docs.getml.com/" + __version__,
    download_url="https://github.com/getml/getml-python-api",
    description="Python API for getML",
    long_description=long_description,
    packages=find_packages(),
    package_data={"getml": ["py.typed", "utilities/templates/*.jinja2"]},
    install_requires=[
        "pandas",
        "pyarrow>=7.0,<7.1",
        "numpy>=1.22",
        "jinja2",
        "scipy",
    ],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    keywords=["AutoML", "feature learning"],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Telecommunications Industry",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Manufacturing",
        "Intended Audience :: Healthcare Industry",
        "Operating System :: OS Independent",
    ],
)
