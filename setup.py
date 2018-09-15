from setuptools import setup, find_packages

setup(
    name='etl-tools',
    version='1.1.2',

    description='Utilities for creating ETL pipelines with mara',

    install_requires=[
        'data_integration>=1.3.0',
    ],

    dependency_links=[
        'git+https://github.com/mara/data-integration.git@1.0.0#egg=data-integration-1.0.0'
    ],

    packages=find_packages(),

    author='Mara contributors',
    license='MIT',

    entry_points={},
)

