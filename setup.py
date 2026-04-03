from setuptools import setup, find_packages

setup(
    name="clinical_inventory",
    version="0.1.0",
    description="Clinical inventory optimization pipeline for Gilead supply chain",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.5.0",
        "numpy>=1.23.0",
        "openpyxl>=3.1.0",
        "pycryptodome>=3.18.0",
    ],
)
