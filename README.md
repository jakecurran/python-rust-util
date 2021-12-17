# Python - Rust Util Example

Util package to demonstrate how to enable Rust code as part of a Python project
using `cpython` and `setuptools-rust`.

- `rust_util`: python interop using [`cpython`](https://docs.rs/cpython)
- `logic`: logic, target agnostic

## Requirements

- Python 3, Pip, Rust (>= 1.56) toolchain

## Installation

### Install dependencies

```bash
pip install -r requirements-dev.txt
```

### Option 1: Directly compile and manually use library file

```bash
> cd rust_util
> cargo build --release
> mv target/release/librust_util.so target/release/rust_util.so
```

Resulting library file (`rust_util.so`) in `rust_util/target` can be used from
its directory as follows

```bash
> cd target/release
> python
Python 3.9.5
[GCC 10.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import rust_util
>>> res = rust_util.scripts.parse_nginx_log(path)
```

### Option 2: Install using Pip

```bash
> pip3 install . # or python3 setup.py install
```

- Package can then be used like any other Pip installed package

## Usage

```python
from rust_util import scripts

result = scripts.parse_nginx_log('/path/to/log/file.log')
```

## Performance

Haskell's `criterion` package was used to test the performance of the sample
Rust code with and without the added cost of Python interoperability. The
results are in `/perf-tests`.
