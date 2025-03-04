FROM ubuntu:jammy

ARG accessKeyId
ARG secretAccessKey
ARG bucketName
ARG yezzeyRef

ENV YEZZEY_REF=${yezzeyRef:-v1.8_opengpdb}


SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive

RUN stat -fc %T /sys/fs/cgroup/

RUN useradd -rm -d /home/gpadmin -s /bin/bash -g root -G sudo -u 1001 gpadmin

RUN ln -snf /usr/share/zoneinfo/Europe/London /etc/localtime && echo Europe/London > /etc/timezone \
&& apt-get update -o Acquire::AllowInsecureRepositories=true && apt-get install -y --no-install-recommends --allow-unauthenticated \
  build-essential libssl-dev gnupg devscripts \
  openssl libssl-dev debhelper debootstrap \
  make equivs bison ca-certificates-java ca-certificates \
  cmake curl cgroup-tools flex gcc-9 g++-9 g++-9-multilib \
  git krb5-multidev libapr1-dev libbz2-dev libcurl4-gnutls-dev \
  libevent-dev libkrb5-dev libldap2-dev libperl-dev libreadline6-dev \
  libssl-dev libxml2-dev libyaml-dev libzstd-dev libaprutil1-dev \
  libpam0g-dev libpam0g libcgroup1 libyaml-0-2 libldap2-dev openssl \
  ninja-build python2-dev python-setuptools quilt unzip wget zlib1g-dev libuv1-dev \
  libgpgme-dev libgpgme11 sudo iproute2 less software-properties-common \
  openssh-client openssh-server

RUN apt-get install -y locales \
&& locale-gen "en_US.UTF-8" \
&& update-locale LC_ALL="en_US.UTF-8"

RUN echo 'gpadmin ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers

USER gpadmin
WORKDIR /home/gpadmin

COPY yezzey_test/generate_ssh_key.sh /home/gpadmin/

RUN ["/home/gpadmin/generate_ssh_key.sh"]


RUN cd /tmp/ \
&& git clone https://github.com/greenplum-db/gp-xerces-archive.git \
&& cd ./gp-xerces-archive/ && mkdir build && cd build && ../configure --prefix=/usr/local && make -j \
&& sudo make install

RUN cd /tmp/ \
&& git clone https://github.com/boundary/sigar.git \
&& cd ./sigar/ \
&& mkdir build && cd build && cmake .. && make \
&& sudo make install

COPY . /home/gpadmin

RUN sudo DEBIAN_FRONTEND=noninteractive ./README.ubuntu22.bash \
&& sudo apt install -y libhyperic-sigar-java libaprutil1-dev libuv1-dev

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
