# Must run scripts logged in as root
yum -y update

yum -y groupinstall "Development Tools"

yum install -y wget

currDir=$(cd $(dirname $0); pwd)
cd ${currDir}
mkdir -p tmpdir
cd tmpdir

wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz
tar xvf autoconf-2.69.tar.gz
cd autoconf-2.69
./configure --prefix=/usr
make -j
make -j install
cd ..

wget http://ftp.gnu.org/gnu/automake/automake-1.14.tar.gz
tar xvf automake-1.14.tar.gz
cd automake-1.14
./configure --prefix=/usr
make -j
make -j install
cd ..

wget http://ftp.gnu.org/gnu/bison/bison-2.5.1.tar.gz
tar xvf bison-2.5.1.tar.gz
cd bison-2.5.1
./configure --prefix=/usr
make -j
make -j install
cd ..

yum -y install libevent-devel zlib-devel openssl-devel

wget http://sourceforge.net/projects/boost/files/boost/1.55.0/boost_1_55_0.tar.gz
tar xvf boost_1_55_0.tar.gz
cd boost_1_55_0
./bootstrap.sh
./b2 install
cd ..

#pushd ../../external/thrift-0.9.1 >/dev/null
#./bootstrap.sh
#yes | cp -rf /usr/bin/libtool ./
#git stash
#./configure CXXFLAGS='-O3 -std=c++11' \
#  --prefix=`pwd`/../../ --exec-prefix=`pwd`/../../ \
#  --with-qt4=no --with-c_glib=no --with-csharp=no --with-java=no \
#  --with-erlang=no --with-python=no --with-perl=no --with-php=no \
#  --with-php_extension=no --with-ruby=no --with-haskell=no \
#  --with-go=no --with-d=no --without-tests
#make -j && make -j install
#popd >/dev/null

git clone https://git-wip-us.apache.org/repos/asf/thrift.git -b 0.9.2
cd thrift
./bootstrap.sh
./configure \
  CXXFLAGS='-O3 -std=c++11' \
  --with-boost-libdir=/usr/local/lib/ \
  --with-qt4=no --with-c_glib=no --with-csharp=no --with-java=no \
  --with-erlang=no --with-python=no --with-perl=no --with-php=no \
  --with-php_extension=no --with-ruby=no --with-haskell=no \
  --with-go=no --with-d=no --with-lua=no \
  --without-tests
make -j
make -j install
cd ..

cd ..
rm -rf tmpdir
