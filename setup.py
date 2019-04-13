from setuptools import setup, find_packages

setup(
    name='etl-tools',

    version='2.0.0',

    description='Utilities for creating ETL pipelines with mara',

    install_requires=[
        'data_integration>=2.0.0',
    ],

    packages=find_packages(),

    author='Mara contributors',
    license='MIT',

    entry_points={},
)

