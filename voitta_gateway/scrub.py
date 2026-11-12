"""Identifiers out of text that is about to be logged (#11).

Phone numbers and Signal ACIs reach logs through other people's text: signal-cli's
own console output, its JSON-RPC error messages, a malformed line. Logs travel
further than gateway.yaml, so those strings are scrubbed on the way in."""
import re

_IDS = re.compile(r"\+?\d[\d\s().-]{6,}\d"
                  r"|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


def ids(text) -> str:
    return _IDS.sub("<id>", str(text))
