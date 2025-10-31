from opshin.prelude import *


def assert_minting_purpose(context: ScriptContext) -> None:
    purpose = context.purpose
    assert isinstance(purpose, Minting), "not minting"


def assert_signed(pkh: PubKeyHash, context: ScriptContext) -> None:
    assert pkh in context.transaction.signatories, "missing signature"


def validator(pkh: PubKeyHash, context: ScriptContext) -> None:
    assert_minting_purpose(context)
    assert_signed(pkh, context)
