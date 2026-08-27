top_builddir = ../..
include $(top_builddir)/src/Makefile.global

all: build_tea/tea/gpext/tea-1.80.0.so

boost-1.81.0.tar.gz:
	wget -O boost-1.81.0.tar.gz https://apache.jfrog.io/artifactory/arrow/thirdparty/7.0.0/boost_1_81_0.tar.gz

cmake-3.31.11-linux-x86_64/bin/cmake:
	wget -O - https://github.com/Kitware/CMake/releases/download/v3.31.11/cmake-3.31.11-linux-x86_64.tar.gz | tar -xzf -

bin/share/arrow/gdb/gdb_arrow.py: cmake-3.31.11-linux-x86_64/bin/cmake
	rm -rf build_arrow
	cmake-3.31.11-linux-x86_64/bin/cmake -B build_arrow -S arrow/cpp \
		-DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=$$PWD/bin \
		-DCMAKE_C_COMPILER=gcc-13 -DCMAKE_CXX_COMPILER=g++-13 \
		-DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF \
		-DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_NO_DEPRECTATED_API=ON \
		-DARROW_LLVM_USE_SHARED=OFF -DARROW_FILESYSTEM=ON -DARROW_PARQUET=ON \
		-DARROW_S3=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_LZ4=ON \
		-DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=ON -DARROW_IPC=ON -DARROW_CSV=ON \
		-DARROW_WITH_RAPIDJSON=ON -DARROW_GANDIVA=ON -DARROW_COMPUTE=ON
	$(MAKE) -C build_arrow
	$(MAKE) -C build_arrow install

build_tea/tea/gpext/tea-1.80.0.so: boost-1.81.0.tar.gz bin/share/arrow/gdb/gdb_arrow.py
	rm -rf build_tea
	ARROW_DEPS=$$PWD cmake-3.31.11-linux-x86_64/bin/cmake -B build_tea -S tea \
		-DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_PREFIX_PATH=$$PWD/bin \
		-DCMAKE_C_COMPILER=gcc-13 -DCMAKE_CXX_COMPILER=g++-13 \
		-DTEA_BUILD_EXT=OFF \
		-DPG_CONFIG=$$PWD/../../src/bin/pg_config/pg_config \
		-DGreenplum_INCLUDE_SERVER=$$PWD/../../src/include \
		-DCMAKE_INSTALL_PREFIX=$(prefix)
	$(MAKE) -C build_tea

clean:
	rm -rf bin boost-1.81.0.tar.gz build_arrow build_tea cmake-3.31.11-linux-x86_64

distclean: clean

install:
	$(MAKE) -C build_tea install
