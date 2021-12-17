#!/usr/bin/env python3

import subprocess
import sys

from setuptools import setup
from setuptools_rust import Binding, RustExtension

RUST_EXTENSIONS = [RustExtension(
    'rust_util', 'rust_util/Cargo.toml', binding=Binding.RustCPython
)]

SETUP_REQUIRES = ['setuptools', 'setuptools-rust', 'wheel']

setup(name='rust_util',
      author='Jake Curran',
      author_email='jake@jakecurran.com',
      description='Util package to enable Rust code as part of a Python project.',
      long_description=open('README.md').read(),
      version='1.0',
      rust_extensions=RUST_EXTENSIONS,
      setup_requires=SETUP_REQUIRES,
      zip_safe=False)
