Implemented Block Based Nested Loop Join Operator in Impala.

To run it, simply include the .cc and .h files in be/exec of Impala code base, compile and execute it.

The block nested loop join algorithm improves on the simple nested loop join by only scanning S once for every group of R tuples. For example, one variant of the block nested loop join reads an entire page of R tuples into memory and loads them into a hash table. It then scans S, and probes the hash table to find S tuples that match any of the tuples in the current page of R. This reduces the number of scans of S that are necessary.

The block nested loop runs in O(P_r P_s/M) I/Os where M is the number of available pages of internal memory and P_r and P_s is size of R and S respectively in pages. Note that block nested loop runs in O(P_r+P_s) I/Os if R fits in the available internal memory.