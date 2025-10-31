#!/usr/bin/env -S opshin eval spending
from opshin.prelude import *


def validator(context: ScriptContext) -> None:
    pubkeyhash: bytes = own_datum_unsafe(context)
    sig_present = pubkeyhash in context.transaction.signatories
    assert (
        sig_present
    ), f"Required signature missing, expected {pubkeyhash.hex()} but got {[s.hex() for s in context.transaction.signatories]}"
