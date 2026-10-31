from setuptools import setup, find_packages

setup(
    name="mdb-python",
    version="0.1.0",
    description="Python driver for MDB - Xinchuang Document Database",
    packages=find_packages(),
    install_requires=["pymongo>=4.6.0"],
    python_requires=">=3.8",
)
