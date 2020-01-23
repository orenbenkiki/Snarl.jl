#!/bin/sh
#{
#FINAL=true

grep -n '^ [ ]*[0-9] ' */*.cov \
| sed 's/\.[0-9][0-9]*\.cov:\([0-9][0-9]*\): [ ]*\([0-9][0-9]*\)/:\1:\2:/' \
| sort -t ':' -k '1,1' -k '2n,2' \
| awk -F ':' '
BEGIN {
    OFS = ":"
    prev_file = ""
    prev_line = -1
    prev_count = -1
    prev_text = ""
}
{
    next_file = $1
    next_line = $2
    next_count = $3
    next_text = $4
    if (next_file == prev_file && next_line == prev_line) {
        prev_count += next_count
    } else {
        if (prev_count >= 0) {
            print prev_file, prev_line, prev_count, prev_text
            print ""
        }
        prev_file = next_file
        prev_line = next_line
        prev_count = next_count
        prev_text = next_text
    }
}
' \
| awk -F ':' '
BEGIN {
    OFS = ":"
    unreached = 0
    reached = 0
}
/Only seems unreached/ { next }
$3 == 0 && /Not reached/ { next }
$3 == 0 {
    print $1, $2, " - " $4
    unreached += 1
}
/Not reached/ && $3 > 0 {
    print $1, $2, " + " $4
    reached += 1
}
END {
    if (unreached > 0) {
        printf("ERROR: %s unreached lines without a NOT REACHED annotation\n", unreached)
    }
    if (reached > 0) {
        printf("ERROR: %s reached lines with a NOT REACHED annotation\n", reached)
    }
    if (unreached > 0 || reached > 0) {
        exit(1)
    }
}
'
