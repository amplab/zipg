#!/bin/bash
set -e

# compile OCaml

wget http://caml.inria.fr/pub/distrib/ocaml-4.02/ocaml-4.02.1.tar.gz
tar xvzf ocaml-4.02.1.tar.gz
cd ocaml-4.02.1

export GNUMAKE=gmake
./configure
make -j8 world
make -j8 opt
make -j8 opt.opt

umask 022
sudo make -j8 install

# others
yum install -y gcc-c++ htop
