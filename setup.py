from setuptools import setup, find_packages
import re

def get_long_description():
    with open('README.md') as f:
        return re.sub('!\[(.*?)\]\(docs/(.*?)\)', r'![\1](https://github.com/mara/mara-etl-tools/raw/master/docs/\2)', f.read())

setup(
    name='mara-etl-tools',

    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    url = 'https://github.com/mara/mara-etl-tools',

    version='4.0.0',

    description='Utilities for creating ETL pipelines with mara',

    install_requires=[
        'mara-pipelines>=3.0.0',
    ],

    python_requires='>=3.6',

    packages=find_packages(),

    author='Mara contributors',
    license='MIT',

    entry_points={},
)

