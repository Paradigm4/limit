# Limit
Return up to K cells from a SciDB array

# Example
```
$ iquery -naq "store(build(<val:double> [x=1:100,1000000,0], random()), temp)"
Query was executed successfully

$ iquery -aq "limit(temp, 5)"
{x} val
{1} 1.01289e+09
{2} 1.50197e+08
{3} 1.80527e+09
{4} 1.43597e+09
{5} 1.2949e+09
```

# Arguments
The sole parameter is a `uint64` number of cells to return. The output schema is always the same as the input schema. Always returns either K cells or the entire array - whichever is smaller. The cells that are chosen to return are indeterminate, and may vary. Optimized for small values of K. Negative inputs and null inputs are coerced to a limit equal to max signed int64 - effectively returning the entire array:
```
$ iquery -aq "op_count(limit(temp, null))"
{i} count
{0} 100
```

# Uses
`limit` is best for quick examination of large arrays. If the array is dense, then a quick `between` is a good option - but if the array is sparse, the user can't predict which region to select, nor how much data would be returned. The op is also useful as a kind of sampling technique when composing queries. For example:
```
complex_query( limit(A, 10000000))
```
as well as
```
limit(complex_query(A), 10000000)
```

# Installation
The easiest way is to first set up dev_tools (https://github.com/paradigm4/dev_tools). Follow the instructions there.
