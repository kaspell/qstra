# Qstra

A persistent, highly concurrent database serving Bloom filters, written in Rust.

## Architecture

To maximize the capacity for concurrent connections, incoming queries are served by a concurrent task queue Queries are decoded and dispatched to fetch the result from the in-memory data store. Queries can be either read or write -type querie. Write -type queries require locking mechanisms that might block other queries, while read -type queries read on a best-effort basis.

Since Bloom filters are space efficient, all data in the current implementation can be represented in-memory while the database server is running. Data is persisted to secondary storage at shutdown during normal operations, and via a write-ahead log (WAL) backups after any uncontrolled shutdowns. All state-changing queries contribute to the WAL through query postprocessing. Custom serialization/deserialization is used for interactions between the in-memory data store and the secondary storage.
<br/>
<br/>

<p align="center">
        <img src="https://raw.githubusercontent.com/kaspell/qstra/refs/heads/main/qstra-arch-diag.svg" alt="Architecture for Qstra" />
</p>

## Bloom filters

### What is a Bloom filter?

A [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) is a probabilistic data structure that answers predicate queries of the form $x \in X$. That is, <i>does this element exist in this set</i>?

It is therefore a partial implementation of the general concept of the "set" that is optimized to provide quick and probabilistic answers to queries of this type. A Bloom filter is only a partial implementation of a set since it doesn't have the other useful and important properties that sets have: for example, ability to query the number of elements in the set, or take set differences.

### How is a Bloom filter implemented?

A Bloom filter is implemented as a [bit array](https://en.wikipedia.org/wiki/Bit_array) and a collection of hash functions.

To instantiate a Bloom filter, one has to choose two parameters: the number of bits in the bit array and the number and the choice of the associated hash functions. Two operations are supported: adding elements to the set and querying whether an element exists in the set. To add an element to the Bloom filter, one sets the bits mapped to by the hash functions to $1$. To answer whether an element exists in the Bloom filter, one checks whether all of the bits mapped to by the hash functions are set to $1$.

To make things concrete, assume that our Bloom filter instantiation $B$ has $64$ bits and two hash functions $f : X \to \\{ 0, 1, \ldots, 63 \\}$ and $g : X \to \\{ 0, 1, \ldots, 63 \\}$ that map any byte sequence in $X$ to the index of one of the bits. The life of $B$ starts as an array of $0$ bits:

```math
0000\ 0000\ 0000\ 0000\ 0000\ 0000\ 0000\ 0000
```
<br/>

If we want to add an element, a byte sequence $x \in X$, to $B$, we set bits $f(x)$ and $g(x)$ in $B$ to $1$. If we want to make a query about the existence of an element $y \in X$ in $B$, we inspect the bits $f(y)$ and $g(y)$: if they are both set to $1$, then the element exists in the set <i>with some probability</i>. To see where this probabilistic guarantee arises from, let's look at a toy example.

Suppose that our hash functions were toy hash functions with (say) $f(x) := \sum_i x_i \mod 32$ and $g(x) := \left( 1 + \sum_i x_i \right) \mod 32$ and our bit indexing was such that the rightmost bit was indexed by $0$. For an element $52 \in X$, we would have $f(52) = 20$ and $g(x) = 21$. After adding this element to $B$, the state of $B$ would be

```math
0000\ 0000\ 0001\ 1000\ 0000\ 0000\ 0000\ 0000
```
<br/>

If we next queried for the existence of element $117$, we would have $f(117) = 21$ and $g(117) = 22$. Only the bit at index $f(117) = 21$ is set to $1$, so the query would be responded in the negative. However, for element $84$, we have exactly $f(84) = 20$ and $g(84) = 21$. Both bits at indices $f(84)$ and $g(84)$ are set to $1$, so in this case the query would be responded in the affirmative, which would result in a <i>false positive</i> response.

This explains the probabilistic nature of Bloom filters. Since  there are way more potential elements in $X$ than there are bits encoding information in $B$, it is inevitable that $f(x) = f(y)$ and $g(x) = g(y)$ for some elements $x, y \in X$ that are not actually equal, $x \neq y$. That is, depending on the parameters the Bloom filter was built with, all positive answers to the queries $x \in X$ are only guaranteed <i>under some probability</i>, which means that for sufficiently large numbers of sufficiently varying queries, false positives are overwhelmingly likely to be encountered. However, by calibrating the parameters correctly for the requirements that the Bloom filter would be expected to perform at, the number of false positives as a fraction of all answers can always be kept arbitrarily low, given sufficient capacity in memory and compute.

False negatives are, however, impossible without some unintended corruption of the bit array bits (whether in the hardware or the software layers), since bits are only set from $0$ to $1$ and never in the other direction. There exists other similar probabilistic data structures that support element removal.

For these reasons, Bloom filters excel in tasks where one wants quick answers to questions that can be modeled as predicates $x \in X$ and where occasional false positive answers are not too detrimental. The questions that can be modeled as $x \in X$ predicates are usually of the form, "have we seen these bytes before?".

## License

This repository is licensed under the [AGPL v3 license](https://github.com/kaspell/qstra/blob/main/LICENSE).