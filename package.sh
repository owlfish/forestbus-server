#!/bin/bash

echo "Building source distribution"
tar -cf forestbus-server-src.tar src/github.com/owlfish/forestbus-server Changes.txt LICENSE.txt README.txt

gzip -9 forestbus-server-src.tar

echo "Building binary distribution"

tar -cf forestbus-server-bin.tar bin Changes.txt LICENSE.txt README.txt

gzip -9 forestbus-server-bin.tar

