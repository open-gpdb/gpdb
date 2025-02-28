FROM gp_build_jammy:1234

ARG accessKeyId
ARG secretAccessKey
ARG bucketName
ARG yezzeyRef

ENV YEZZEY_REF=${yezzeyRef:-v1.8_opengpdb}


SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive

COPY yezzey_test/generate_ssh_key.sh /home/gpadmin/
RUN /home/gpadmin/generate_ssh_key.sh

USER gpadmin
WORKDIR /home/gpadmin

COPY . /home/gpadmin

RUN sudo mkdir /usr/local/gpdb \
&& sudo chown gpadmin:root /usr/local/gpdb

RUN sudo chown -R gpadmin:root /home/gpadmin \
&& git status

RUN  git submodule update --init 
RUN rm -fr gpcontrib/yezzey 

RUN git clone https://github.com/open-gpdb/yezzey.git gpcontrib/yezzey -b ${YEZZEY_REF} && cd /home/gpadmin 

RUN sed -i '/^trusted/d' gpcontrib/yezzey/yezzey.control 
RUN ./configure --with-perl --with-python --with-libxml --prefix=/usr/local/gpdb --enable-orafce \
--enable-depend --enable-cassert --without-mdblocales --with-zstd --enable-debug-extensions --enable-orca CFLAGS='-fno-omit-frame-pointer -Wno-implicit-fallthrough -O3 -pthread' 
RUN make -j8 && make -j8 install

ENTRYPOINT ["./docker/test/run_tests.sh"]
