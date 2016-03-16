#!/bin/bash

iquery -anq "remove(zero_to_255)"                > /dev/null 2>&1
iquery -anq "store( build( <val:string> [x=0:255,10,0],  string(x % 256) ), zero_to_255 )"  > /dev/null 2>&1

rm test.out
rm test.expected

touch ./test.expected
iquery -o csv:l -aq "op_count(limit(zero_to_255, null))" >> test.out
echo 'count' >> ./test.expected
echo '256' >> ./test.expected

iquery -o csv:l -aq "op_count(limit(zero_to_255, 23))" >> test.out
echo 'count' >> ./test.expected
echo '23' >> ./test.expected

iquery -o csv:l -aq "op_count(limit(zero_to_255, 7))" >> test.out
echo 'count' >> ./test.expected
echo '7' >> ./test.expected

iquery -o csv:l -aq "op_count(limit(zero_to_255, 0))" >> test.out
echo 'count' >> ./test.expected
echo '0' >> ./test.expected

diff test.out test.expected
exit 0

