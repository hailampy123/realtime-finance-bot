"""Re-derive history from Binance's public archive (spec §6).

Durability here is re-derivation, not backup: the sandbox account is wiped every
seven days, so this package is load-bearing infrastructure rather than a history
nicety. Everything under it is either pure and offline-testable, or thin I/O.

Layering, and the reason for it:

    epoch.py      the timestamp-unit trap. Pure.
    parsers.py    CSV -> typed rows, including the isBuyerMaker trap. Pure.
    tiers.py      which archive files a window needs, and their URLs. Pure.
    checksum.py   .CHECKSUM parse and verify. Pure.
    manifest.py   manifest rows for a window. Pure.
    staging.py    normalised rows -> gzipped CSV bytes. Pure.
    loader.py     the Lambda handler. The only module that touches the network.

Everything except loader.py is a pure function of its inputs, which is what lets
the two parsing traps of §6.3 be caught by offline golden-file tests rather than
by a wrong backtest six months later.
"""
