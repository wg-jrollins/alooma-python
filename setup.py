#!/usr/bin/env python
from distutils.core import setup

setup(name='alooma',
      version='0.1.1',
      description='Alooma python API',
      author='Yonatan Kiron',
      author_email='yonatan@alooma.io',
      packages=['alooma'],
      install_requires=[
        "urllib3>=1.12",
        "requests>=2.8.1",
        "paramiko>=1.16.0"
      ],
      keywords=['alooma']
)
