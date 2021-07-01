#!/bin/sh
rm -rf deps/build
ln -sf $PWD/src $PWD/deps/src
julia --color=yes deps/document.jl
rm deps/src
