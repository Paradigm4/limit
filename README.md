# limit
A simple operator to return the first K cells of an array

## Suggested Approach
```
limit(input, K)
  input: any SciDB array
  K    : uint64 constant
  returns an array with the same dimensions and attributes as input, max(K, count(input)) cells from input, selected arbitrarily. 
  running time: O(K)
```

## Simple algo:
Phase 1: On each instance:
 1. create new MemArray m with same schema as input
 2. while input has more cells and local count ( m) < K
     1. copy cell c from input to m

Phase 2: redistribute m to coordinator (psLocalInstance)

Phase 3: repeat Phase 1 on coordinator to create new m', return m'.

The algo can then be improved to make the phases streaming for better performance.
