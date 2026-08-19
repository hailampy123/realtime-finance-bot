"""A static dashboard over the data layer: is it healthy, and what did it buy?

Two jobs, deliberately in one page:

    Health    row counts by layer, freshness, quarantine rate. Answers "is the
              pipeline running and is anything being silently dropped?"
    Showcase  mark price, funding rate and open interest for one instrument.
              Answers "what does the enrichment let me see that the tape did
              not?" -- price rising on rising open interest is new money; rising
              on falling open interest is shorts covering.

    charts.py  pure SVG builders. No Athena, no clock, no I/O.
    page.py    pure HTML assembly. Takes rows, returns a self-contained document.
    cli.py     the only module that runs a query or writes a file.

Everything except cli.py is a pure function of its inputs, so the page renders in
a unit test with no AWS account.
"""
