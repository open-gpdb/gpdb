FROM ubuntu:focal

ARG accessKeyId
ARG secretAccessKey
ARG bucketName
ARG yezzeyRef

ENV AWS_ACCESS_KEY_ID=${accessKeyId}
ENV AWS_SECRET_ACCESS_KEY=${secretAccessKey}
ENV S3_BUCKET=${bucketName}
ENV WALG_S3_PREFIX=s3://${bucketName}/yezzey-test-files

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive

RUN useradd -rm -d /home/krebs -s /bin/bash -g root -G sudo -u 1001 krebs

RUN ln -snf /usr/share/zoneinfo/Europe/London /etc/localtime && echo Europe/London > /etc/timezone \
&& apt-get update -o Acquire::AllowInsecureRepositories=true && apt-get install -y --no-install-recommends --allow-unauthenticated \
  build-essential libssl-dev gnupg devscripts \
  openssl libssl-dev debhelper debootstrap \
  make equivs bison ca-certificates-java ca-certificates \
  cmake curl cgroup-tools flex gcc-8 g++-8 g++-8-multilib \
  git krb5-multidev libapr1-dev libbz2-dev libcurl4-gnutls-dev \
  libevent-dev libkrb5-dev libldap2-dev libperl-dev libreadline6-dev \
  libssl-dev libxml2-dev libyaml-dev libzstd-dev libaprutil1-dev \
  libpam0g-dev libpam0g libcgroup1 libyaml-0-2 libldap-2.4-2 libssl1.1 \
  ninja-build python-dev python-setuptools quilt unzip wget zlib1g-dev libuv1-dev \
  libgpgme-dev libgpgme11 sudo iproute2 less software-properties-common \
  openssh-client openssh-server

COPY yezzey_test/install-wal-g.sh /home/krebs

RUN ["/home/krebs/install-wal-g.sh"]

RUN apt-get install -y locales \
&& locale-gen "en_US.UTF-8" \
&& update-locale LC_ALL="en_US.UTF-8"

RUN echo 'krebs ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers

USER krebs
WORKDIR /home/krebs

COPY yezzey_test/import_gpg_keys.sh /home/krebs/
COPY yezzey_test/priv.gpg /home/krebs/yezzey_test/priv.gpg
COPY yezzey_test/pub.gpg /home/krebs/yezzey_test/pub.gpg

RUN ["/home/krebs/import_gpg_keys.sh"]

COPY yezzey_test/generate_ssh_key.sh /home/krebs/

RUN ["/home/krebs/generate_ssh_key.sh"]


RUN cd /tmp/ \
&& git clone https://github.com/boundary/sigar.git \
&& cd ./sigar/ \
&& mkdir build && cd build && cmake .. && make \
&& sudo make install

COPY . /home/krebs

RUN sudo DEBIAN_FRONTEND=noninteractive ./README.ubuntu.bash \
&& sudo apt install -y libhyperic-sigar-java libaprutil1-dev libuv1-dev

RUN sudo mkdir /usr/local/gpdb \
&& sudo chown krebs:root /usr/local/gpdb

RUN sudo chown -R krebs:root /home/krebs \
&& git status

RUN  git submodule update --init 
RUN cd gpcontrib/yezzey 
RUN git checkout ${yezzeyRef} && cd /home/krebs 
RUN sed -i '/^trusted/d' gpcontrib/yezzey/yezzey.control 
RUN ./configure --with-perl --with-python --with-libxml --disable-orca --prefix=/usr/local/gpdb \
--enable-depend --enable-cassert --enable-debug --without-mdblocales --without-zstd CFLAGS='-fno-omit-frame-pointer -Wno-implicit-fallthrough -O3 -pthread' 
RUN make -j8 && make -j8 install

RUN sed -i "s/\$ACCESS_KEY_ID/${accessKeyId}/g" yezzey_test/yezzey-s3.conf \
&& sed -i "s/\$SECRET_ACCESS_KEY/${secretAccessKey}/g" yezzey_test/yezzey-s3.conf \
&& sed -i "s/\$AWS_ACCESS_KEY_ID/${accessKeyId}/g" yezzey_test/wal-g-conf.yaml \
&& sed -i "s/\$AWS_SECRET_ACCESS_KEY/${secretAccessKey}/g" yezzey_test/wal-g-conf.yaml \
&& sed -i "s/\$WALG_S3_PREFIX/s3:\/\/${bucketName}\/yezzey-test-files/g" yezzey_test/wal-g-conf.yaml

ENTRYPOINT ["./yezzey_test/run_tests.sh"]
