from setuptools import setup, find_packages

setup(
    name="pyspark_sample",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark==3.4.1",
        "delta-spark==2.4.0",
    ],
)
