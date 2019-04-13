from setuptools import setup, find_packages

setup(
    name='etl-tools',

    version='1.2.1',

    description='Utilities for creating ETL pipelines with mara',

    install_requires=[
        'data_integration>=1.3.0',
    ],

    packages=find_packages(),

    author='Mara contributors',
    license='MIT',

    entry_points={},
)

