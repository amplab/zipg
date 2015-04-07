#!/usr/bin/env python
import sys, string, itertools

if len(sys.argv) < 2:
    sys.stderr.write('Usage: [file_name] [name_size]')
    sys.exit(1)

name = sys.argv[1]
size = int(sys.argv[2])
alphabet = string.ascii_letters + string.digits

file = open(name + '.txt', 'w')
for name in itertools.imap(''.join, itertools.product(alphabet, repeat=size)):
    file.write("%s\n" % name)

file.close();
