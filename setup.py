#!/usr/bin/env python
from pip.req import parse_requirements

install_reqs = parse_requirements("requirements.txt", session=False)

reqs = [str(ir.req) for ir in install_reqs]

from distutils.core import setup

setup(name='alooma',
      version='1.0.0',
      description='Alooma Python API',
      author='Yonatan Kiron',
      author_email='yonatan@alooma.com',
      packages=['alooma', 'alooma.submodules'],
      install_requires=reqs,
      keywords=['alooma']
)
