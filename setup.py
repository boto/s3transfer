#!/usr/bin/env python
import os
import re
import sys

from setuptools import setup, find_packages


ROOT = os.path.dirname(__file__)
VERSION_RE = re.compile(r'''__version__ = ['"]([0-9.]+)['"]''')


requires = [
    'botocore>=1.3.0,<2.0.0',
]


if sys.version_info[0] == 2:
    # concurrent.futures is only in python3, so for
    # python2 we need to install the backport.
    requires.append('futures>=2.2.0,<4.0.0')


def get_version():
    init = open(os.path.join(ROOT, 's3transfer', '__init__.py')).read()
    return VERSION_RE.search(init).group(1)


setup(
    name='s3transfer',
    version=get_version(),
    description='An Amazon S3 Transfer Manager',
    long_description=open('README.rst').read(),
    author='Amazon Web Services',
    author_email='kyknapp1@gmail.com',
    url='https://github.com/boto/s3transfer',
    packages=find_packages(exclude=['tests*']),
    include_package_data=True,
    install_requires=requires,
    extras_require={
        ':python_version=="2.6" or python_version=="2.7"': [
            'futures>=2.2.0,<4.0.0']
    },
    license="Apache License 2.0",
    classifiers=(
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ),
)
