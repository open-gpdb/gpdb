## Building Greenplum Database with GPORCA

GPORCA is a cost-based optimizer which is used by Greenplum Database in
conjunction with the PostgreSQL planner.  It is also known as just ORCA, and
Pivotal Optimizer. The code for GPORCA resides src/backend/gporca. It is built
automatically by default.

### Installing dependencies (for macOS developers)
Follow [these macOS steps](README.macOS.md) for getting your system ready for GPDB

### Installing dependencies (for Linux developers)
Follow [appropriate linux steps](README.linux.md) for getting your system ready for GPDB

## xerces

ORCA requires xerces 3.1 or gp-xerces. For the most up-to-date way of
building gp-xerces, see the README at the following repository:

* https://github.com/greenplum-db/gp-xerces-archive

### Build the database

```
# Configure build environment to install at /usr/local/gpdb
./configure --with-perl --with-python --with-libxml --with-gssapi --prefix=/usr/local/gpdb --without-mdblocales
# Compile and install
make -j8
make -j8 install
# Bring in greenplum environment into your running shell
source /usr/local/gpdb/greenplum_path.sh
# Start demo cluster
make create-demo-cluster
# (gpdemo-env.sh contains __PGPORT__ and __MASTER_DATA_DIRECTORY__ values)
source gpAux/gpdemo/gpdemo-env.sh
```

The directory and the TCP ports for the demo cluster can be changed on the fly.
Instead of `make cluster`, consider:

```
DATADIRS=/tmp/gpdb-cluster PORT_BASE=5555 make cluster
```

The TCP port for the regression test can be changed on the fly:

```
PGPORT=5555 make installcheck-world
```

To turn GPORCA off and use Postgres planner for query optimization:
```
set optimizer=off;
```

If you want to clean all generated files
```
make distclean
```

## Running tests

### Untrusted procedural language extensions

Untrusted PL/Perl and PL/Python extension files are not installed by default.
To run the PL/Perl regression suite (`src/pl/plperl`, including `plperlu`
and mixed `plperl`/`plperlu` tests), the PL/Python regression suite
(`src/pl/plpython`), or other tests that create functions in these languages,
add `--enable-pl-untrusted-extensions` to your configure options, then rebuild
and install before running the tests. This also applies to
`installcheck-world`, which includes these suites when built with Perl and
Python support.

For example, for a separate test installation:

```bash
./configure --with-perl --with-python --with-libxml --with-gssapi \
    --prefix=/usr/local/gpdb-test --without-mdblocales \
    --enable-pl-untrusted-extensions --enable-debug-extensions
make -j8
make -j8 install
source /usr/local/gpdb-test/greenplum_path.sh
make create-demo-cluster
source gpAux/gpdemo/gpdemo-env.sh
make installcheck-world
```

Preserve any other configure options and dependency settings required by your
environment. Use a disposable test cluster. The separate
`--enable-debug-extensions` option installs test extensions such as
`gp_inject_fault`, which are also required by parts of the regression suite.

With Python 2, the untrusted language option installs `plpythonu` and
`plpython2u`; with Python 3, it installs `plpython3u`. With Perl support, it
also installs `plperlu`. It does not change installation of trusted `plperl`.

To verify the default configuration without these extensions, use a separate,
clean installation prefix without `--enable-pl-untrusted-extensions`.
Reinstalling into an existing prefix does not remove extension files left by
an earlier installation.

* The default regression tests

```
make installcheck-world
```

* The top-level target __installcheck-world__ will run all regression
  tests in GPDB against the running cluster. For testing individual
  parts, the respective targets can be run separately.

* The PostgreSQL __check__ target does not work. Setting up a
  Greenplum cluster is more complicated than a single-node PostgreSQL
  installation, and no-one's done the work to have __make check__
  create a cluster. Create a cluster manually or use gpAux/gpdemo/
  (example below) and run the toplevel __make installcheck-world__
  against that. Patches are welcome!

* The PostgreSQL __installcheck__ target does not work either, because
  some tests are known to fail with Greenplum. The
  __installcheck-good__ schedule in __src/test/regress__ excludes those
  tests.

* When adding a new test, please add it to one of the GPDB-specific tests,
  in greenplum_schedule, rather than the PostgreSQL tests inherited from the
  upstream. We try to keep the upstream tests identical to the upstream
  versions, to make merging with newer PostgreSQL releases easier.

## Alternative Configurations

### Building GPDB without GPORCA

Currently, GPDB is built with GPORCA by default. If you want to build GPDB
without GPORCA, configure requires `--disable-orca` flag to be set.
```
# Clean environment
make distclean
# Configure build environment to install at /usr/local/gpdb
./configure --disable-orca --with-perl --with-python --with-libxml --prefix=/usr/local/gpdb
```

### Building GPDB with gpperfmon enabled

gpperfmon tracks a variety of queries, statistics, system properties, and metrics.
To build with it enabled, change your `configure` to have an additional option
`--enable-gpperfmon`

See [more information about gpperfmon here](gpAux/gpperfmon/README.md)

gpperfmon is dependent on several libraries like apr, apu, and libsigar

### Building GPDB with Python3 enabled

GPDB supports Python3 with plpython3u UDF

See [how to enable Python3](src/pl/plpython/README.md) for details.


### Building GPDB client tools on Windows

See [Building GPDB client tools on Windows](README.windows.md) for details.

## Development with Docker

See [README.docker.md](README.docker.md).

We provide a docker image with all dependencies required to compile and test
GPDB [(See Usage)](src/tools/docker/README.md). You can view the dependency dockerfile at `./src/tools/docker/centos6-admin/Dockerfile`.
The image is hosted on docker hub at `pivotaldata/gpdb-dev:centos6-gpadmin`.

A quickstart guide to Docker can be found on the [Pivotal Engineering Journal](http://engineering.pivotal.io/post/docker-gpdb/).

## Development with Vagrant

There is a Vagrant-based [quickstart guide for developers](src/tools/vagrant/README.md).
