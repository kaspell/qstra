# Qstra

A persistent, highly concurrent database serving Bloom filters, written in Rust.

## Bloom filters

### What is a Bloom filter?

A [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) is a probabilistic data structure that answers predicate queries of the form $x \in X$. That is, <i>does this element exist in this set</i>?

It is therefore a partial implementation of the general concept of the "set" that is optimized to provide quick and probabilistic answers to queries of this type. A Bloom filter is only a partial implementation of a set since it doesn't have the other useful and important properties that sets have: for example, ability to query the number of elements in the set, or take set differences.

### How is a Bloom filter implemented?

A Bloom filter is implemented as a [bit array](https://en.wikipedia.org/wiki/Bit_array) and a collection of hash functions.

To instantiate any Bloom filter, one has to choose two parameters: the number of bits in the bit array and the number and the choice of the hash functions involved. To add an element to the Bloom filter, one sets the bits indicated by the hash functions to $1$, and to answer whether an element exists in the Bloom filter, one checks whether all of the bits mapped to by the hash functions are set to $1$.

To make things concrete, assume that our Bloom filter instantiation $B$ has $50$ bits and two hash functions $f : X \to \\{ 0, 1, \ldots, 49 \\}$ and $g : X \to \\{ 0, 1, \ldots, 49 \\}$ that map any byte sequence in $X$ to the index of one of the bits. The life of $B$ starts as an array of 0 bits:

```math
00000000000000000000000000000000000000000000000000
```
<br/>

If we want to add an element, a byte sequence $x \in X$, to $B$, we set bits $f(x)$ and $g(x)$ in $B$ to $1$. If we want to make a query about the existence of an element $y \in X$ in $B$, we inspect the bits $f(y)$ and $g(y)$: if they are both set to $1$, then the element exists in the set <i>with some probability</i>.

This is where the probabilistic nature of Bloom filters come in. Since  there are way more potential elements in $X$ than there are bits encoding information in $B$, it is inevitable that $f(x) = f(y)$ and $g(x) = g(y)$ for some elements $x, y \in X$ that are not actually equal, $x \neq y$. That is, depending on the parameters the Bloom filter was built with, all positive answers to the queries $x \in X$ are only guaranteed <i>under some probability</i>, which means that there is a possibility for <i>false positive</i> answers.

False negatives are, however, impossible without some unintended corruption of the bit array bits (whether in the hardware or the software layers), since bits are only set from $0$ to $1$ and never in the other direction.

For these reasons, Bloom filters excel in tasks where one wants quick answers to questions that can be modeled as predicates $x \in X$ and where occasional false positive answers are not too detrimental. The questions that can be modeled as $x \in X$ predicates are usually of the form, "have we seen these bytes before?".

## Architecture

To maximize the capacity for concurrent connections, incoming queries are served by a concurrent task queue Queries are decoded and dispatched to fetch the result from the in-memory data store. Queries can be either read or write -type querie. Write -type queries require locking mechanisms that might block other queries, while read -type queries read on a best-effort basis.

Since Bloom filters are space efficient, all data in the current implementation can be represented in-memory while the database server is running. Data is persisted to secondary storage at shutdown during normal operations, and via a write-ahead log (WAL) backups after any uncontrolled shutdowns. All state-changing queries contribute to the WAL through query postprocessing. Custom serialization/deserialization is used for interactions between the in-memory data store and the secondary storage.
<br/>
<br/>

<p align="center">
        <img src="https://raw.githubusercontent.com/kaspell/qstra/refs/heads/test_branch/qstra-arch-diag.svg" alt="Architecture for Qstra" />
</p>

## License

This repository is licensed under the [AGPL v3 license](https://github.com/kaspell/qstra/blob/main/LICENSE).