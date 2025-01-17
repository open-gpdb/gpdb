
apt-get update
apt-get install -y \
  bison \
  ccache \
  cmake \
  curl \
  flex \
  inetutils-ping \
  krb5-kdc \
  krb5-admin-server \
  libapr1-dev \
  libbz2-dev \
  libcurl4-gnutls-dev \
  libevent-dev \
  libkrb5-dev \
  libpam-dev \
  libperl-dev \
  libreadline-dev \
  libssl-dev \
  libxml2-dev \
  libyaml-dev \
  libzstd-dev \
  locales \
  net-tools \
  ninja-build \
  openssh-client \
  openssh-server \
  openssl \
  python2-dev \
  python3-pip \
  zlib1g-dev

ln -s /bin/python2.7 /usr/bin/python
curl https://bootstrap.pypa.io/pip/2.7/get-pip.py --output get-pip.py
python get-pip.py
pip2 install conan
pip2 install psutil
rm ./get-pip.py
