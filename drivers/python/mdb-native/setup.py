from setuptools import setup, find_packages

setup(
    name="mdb-native",
    version="0.1.0",
    description="Pure Python MongoDB-compatible driver with native BSON and wire protocol",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=[],
    extras_require={
        "crc32c": ["crc32c"],
    },
)
