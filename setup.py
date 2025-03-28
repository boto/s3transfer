#!/usr/bin/env python
import re
from pathlib import Path
from setuptools import find_packages, setup

ROOT = Path(__file__).parent
VERSION_RE = re.compile(r'''__version__ = ['"]([0-9.]+)['"]''')

requires = [
    'botocore>=1.37.4,<2.0a.0',
]


def get_version():
    init = (ROOT / 's3transfer' / '__init__.py').read_text()
    return VERSION_RE.search(init).group(1)


setup(
    name='s3transfer',
    version=get_version(),
    description='An Amazon S3 Transfer Manager',
    long_description=(ROOT / 'README.rst').read_text(),
    author='Amazon Web Services',
    author_email='kyknapp1@gmail.com',
    url='https://github.com/boto/s3transfer',
    packages=find_packages(exclude=['tests*']),
    include_package_data=True,
    install_requires=requires,
    extras_require={
        'crt': 'botocore[crt]>=1.37.4,<2.0a.0',
    },
    license="Apache License 2.0",
    python_requires=">= 3.8",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
    ],
)
