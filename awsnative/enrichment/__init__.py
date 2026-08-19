"""Derivatives context and macro regime for the data layer (slices E1 and E3).

Design: docs/superpowers/specs/2026-08-17-data-layer-enrichment-derivatives-and-macro-design.md

Gold holds price, size and side. That is the tape, and a decision needs two
things the tape cannot state: whether the market is crowded, and whether a move
is a crypto signal or a beta signal. E1 answers the first from Binance's
perpetual endpoints; E3 answers the second from FRED.

    perp.py     Binance perpetual context -> one wide row per instrument. Pure.
    macro.py    FRED/ALFRED CSV -> vintage-stamped observations. Pure.
    collect.py  the two Lambda handlers. The only module that touches the network.

Everything except collect.py is a pure function of its inputs, so the two
correctness properties that matter -- the 5-minute grid and the vintage
boundary -- are asserted offline.
"""
