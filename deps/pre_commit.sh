#!/bin/sh
set -e
deps/unindexed_files.sh
if grep -i 'TODO'"X" `git ls-files`
then
    exit 1
fi
deps/test.sh
deps/untested_lines.sh
deps/line_coverage.sh
