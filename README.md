# psycopg2 - Python-PostgreSQL Database Adapter

Psycopg is the most popular PostgreSQL database adapter for the Python
programming language. Its main features are the complete implementation of
the Python DB API 2.0 specification and the thread safety (several threads can
share the same connection). It was designed for heavily multi-threaded
applications that create and destroy lots of cursors and make a large number
of concurrent "INSERT"s or "UPDATE"s.

Psycopg 2 is mostly implemented in C as a libpq wrapper, resulting in being
both efficient and secure. It features client-side and server-side cursors,
asynchronous communication and notifications, "COPY TO/COPY FROM" support.
Many Python types are supported out-of-the-box and adapted to matching
PostgreSQL data types; adaptation can be extended and customized thanks to a
flexible objects adaptation system.

Psycopg 2 is both Unicode and Python 3 friendly.

## Documentation

Documentation is included in the `doc` directory and is [available online](https://www.psycopg.org/docs/).

For any other resource (source code repository, bug tracker, mailing list)
please check the [project homepage](https://psycopg.org/).

## Installation

Building Psycopg requires a few prerequisites (a C compiler, some development
packages): please check the [install](https://www.psycopg.org/docs/install.html#install-from-source) and [faq](https://www.psycopg.org/docs/faq.html#faq-compile) documents in the `doc` dir
or online for the details.

If prerequisites are met, you can install from source:

```bash
python setup.py build
sudo python setup.py install
```

Or with pip in editable mode:

```bash
pip install -e .
```

## XCluster DR

This fork adds xCluster DR failover detection capability via `psycopg2.XClusterDRPool`. XCluster DR
must already be configured and running in automatic mode.
