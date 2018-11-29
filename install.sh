#!/bin/bash

# install importlib
echo ">>>>>> to install importlib-1.0.3"
cd importlib-1.0.3
python setup.py install
if [ $? != 0 ] ; then
    echo ">>>>>> install importlib-1.0.3 failed"
    exit 1
fi
echo ">>>>>> install importlib-1.0.3 success"

cd ..

# install importlib
echo ">>>>>> to install setproctitle-1.1.10"
cd setproctitle-1.1.10
python setup.py install
if [ $? != 0 ] ; then
    echo "install setproctitle-1.1.10 failed"
    exit 1
fi
echo ">>>>>> to install setproctitle-1.1.10 success"
