## Bloom filters

### What is a Bloom filter?

A [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) is a probabilistic data structure that answers predicate queries of the form $x \in X$. That is, <i>does this element exist in this set</i>?

It is therefore a partial implementation of the general concept of the "set" that is optimized to provide quick and probabilistic answers to queries of this type. A Bloom filter is only a partial implementation of a set since it doesn't have the other useful and important properties that sets have: for example, ability to query the number of elements in the set, or take set differences.

### How is a Bloom filter implemented?

A Bloom filter is implemented as a [bit array](https://en.wikipedia.org/wiki/Bit_array) and a collection of hash functions.

Any instantiation of a Bloom filter has two parameters involved: the number of bits in the bit array and the number and the choice of the hash functions.

To make things concrete, assume that our Bloom filter instantiation $B$ has $50$ bits and two hash functions $f : \mathbb{B} \to \{ 0, 1, \, \ldots, \, 49 \}$ and $g : \mathbb{B} \to \{ 0, 1, \, \ldots, \, 49 \}$ that map any byte sequence in $\mathbb{B}$ to the index of one of the bits. The life of the Bloom filter starts as an array of 0 bits:

```math
00000000000000000000000000000000000000000000000000
```

If we want to add an element, a byte sequence $x \mathbb{B}$, to $B$, we set bits $f(x)$ and $g(x)$ in $B$ to $1$. If we want to make a query about the existence of an element $y \in \mathbb{B}$ in $B$, we inspect the bits $f(y)$ and $g(y)$: if they are both set to $1$, then the element exists in the set with some probability.

This is where the probabilistic nature of Bloom filters come in. Since  there are way more potential elements in $\mathbb{B}$ than there are bits holding information in $B$, it is inevitable that $f(x) = f(y)$ and $g(x) = g(y)$ for some elements $x, y \in \mathbb{B}$ that are not actually equal $x \neq y$. That is, depending on the parameters the Bloom filter was built with, all positive answers to the queries $x \in X$ are only guaranteed under some probability, which means that there is a possibility for <i>false positive</i> answers.

False negatives are, however, impossible without some unintended corruption of the bit array bits, since bits are only set from $0$ to $1$ and never in the other direction.