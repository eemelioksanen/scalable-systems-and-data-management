"""Batch reference implementation of Query 1 (EMAs) and Query 2 (crossover advisories).

The oracle is built from the recorded contract only (docs/data-contract.md, docs/decisions.md,
docs/interfaces.md), never from the Flink job's code, so that agreement between the two is
evidence rather than a shared bug.
"""

__version__ = "1.0.0"
