#!/usr/bin/env python
import sys, string, itertools

if len(sys.argv) < 1:
    sys.stderr.write('Usage: [name_size]')
    sys.exit(1)

size = int(sys.argv[1])
alphabet = string.ascii_letters + string.digits

file = open('names.txt', 'w')
for name in itertools.imap(''.join, itertools.product(alphabet, repeat=size)):
    file.write("%s\n" % name)

file.close();
