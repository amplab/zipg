#!/bin/bash
set -e

ln -sf ~/spark-ec2/slaves conf/hosts

sbin/copy-dir --delete ./
sbin/copy-dir --delete ~/.bashrc
sbin/copy-dir --delete ~/.gitconfig


