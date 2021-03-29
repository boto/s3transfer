#!/usr/bin/env python
import os
import re
import sys

from setuptools import setup, find_packages


ROOT = os.path.dirname(__file__)
VERSION_RE = re.compile(r'''__version__ = ['"]([0-9.]+)['"]''')


requires = [
    'botocore>=1.12.36,<2.0a.0',
]

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
        ':python_version=="2.7"': [
            'futures>=2.2.0,<4.0.0'],
        'crt': 'botocore[crt]>=1.20.29,<2.0a.0',
    },
    license="Apache License 2.0",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
