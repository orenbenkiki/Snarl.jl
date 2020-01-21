#!/bin/sh
{
FINAL=true
grep -n '^ [ ]*0 ' */*.cov \
| grep -v 'NOT REACHED' \
| sed 's/\.[0-9][0-9]*\.cov:\([0-9][0-9]*\): [ ]*0 /:\1: - /' \
| sort -t ':' -k '1,1' -k '2n,2' \
| uniq -c \
| awk '$1 == 1 {print}' \
| sed 's/^ [ ]*1 [ ]*//;s/\.jl: [ ]*/.jl:/'
if [ $PIPESTATUS -eq 0 ]
then
    FINAL=false
fi

grep -n '^ [ ]*[1-9][0-9]* .*NOT REACHED' */*.cov \
| sed 's/\.[0-9][0-9]*\.cov:\([0-9][0-9]*\): [ ]*[1-9][0-9]* /:\1: + /' \
| sort -t ':' -k '1,1' -k '2n,2' \
| uniq -c \
| awk '$1 == 1 {print}' \
| sed 's/^ [ ]*1 [ ]*//;s/\.jl: [ ]*/.jl:/'
if [ $PIPESTATUS -eq 0 ]
then
    FINAL=false
fi
$FINAL
} 2>&1 \
| sort -t ':' -k '1,1' -k '2n,2'
test $PIPESTATUS -eq 0
