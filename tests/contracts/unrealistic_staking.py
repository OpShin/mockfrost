from opshin.prelude import *


def validator(address: Address, context: ScriptContext) -> None:
    purpose = context.purpose
    if isinstance(purpose, Publishing):
        return None  # Do whatever you like with certifiying
    elif isinstance(purpose, Withdrawing):
        withdrawal_amount = context.transaction.withdrawals[purpose.staking_credential]
        paid_to_address = all_tokens_locked_at_address(
            context.transaction.outputs, address, Token(b"", b"")
        )
        assert (
            paid_to_address >= 2 * withdrawal_amount
        ), "Insufficient rewards to address"
    else:
        assert False, "not a valid purpose"
