#!/usr/bin/env python
from distutils.core import setup

setup(name='alooma',
      version='0.1.3',
      description='Alooma python API',
      author='Yonatan Kiron',
      author_email='yonatan@alooma.io',
      packages=['alooma'],
      install_requires=[
        "urllib3>=1.13",
        "requests>=2.9.0",
        "paramiko>=1.16.0"
      ],
      keywords=['alooma']
)
