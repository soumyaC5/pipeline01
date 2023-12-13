"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup
from v1 import __version__

PACKAGE_REQUIREMENTS = ["pyyaml"]

# packages for local development and unit testing
# please note that these packages are already available in DBR, there is no need to install them on DBR.
LOCAL_REQUIREMENTS = [
    "pyspark==3.2.1",
    "delta-spark==1.1.0",
    "boto3",
    "scikit-learn",
    "databricks-sdk",
    "databricks-feature-store",
    "evidently",
    "pandas",
    "mlflow",
    "urllib3"
    
]

TEST_REQUIREMENTS = [
    # development & testing tools
    "pytest",
    "coverage[toml]",
    "pytest-cov",
    "dbx>=0.8"
]

setup(
    name="v1",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["setuptools","wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    extras_require={"local": LOCAL_REQUIREMENTS, "test": TEST_REQUIREMENTS},
    entry_points = {
        "data_prep = v1.tasks.data_prep:entrypoint",
            "model_train = v1.tasks.model_train:entrypoint", 
            "model_inference = v1.tasks.model_inference:entrypoint",
            "webhook = v1.tasks.webhook:entrypoint",
    },
    version=__version__,
    description="",
    author="",
)
