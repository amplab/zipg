git clone https://git-wip-us.apache.org/repos/asf/thrift.git
cd thrift
./bootstrap.sh
./configure --with-lua=no
make
make install
cd ..
