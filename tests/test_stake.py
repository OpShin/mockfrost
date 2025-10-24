import pathlib
import unittest

import pycardano
import pytest
from pycardano import TransactionFailedException

from plutus_bench import MockChainContext, MockUser, MockPool
from plutus_bench.mock import MockFrostApi

from tests.stake import register_and_delegate, withdraw
from pycardano.crypto.bech32 import decode
from opshin import build
from opshin.ledger.api_v3 import (
    Address as Address,
    PubKeyCredential,
    PubKeyHash,
    NoStakingCredential,
)

own_path = pathlib.Path(__file__)


def as_ledger_address(address: pycardano.Address) -> Address:
    return Address(
        PubKeyCredential(PubKeyHash(address.payment_part.payload)),
        NoStakingCredential(),
    )


def test_register_and_delegate():
    api = MockFrostApi()
    context = MockChainContext(api=api)
    staking_user = MockUser(api)
    stake_pool = MockPool(api)

    staking_user.fund(100_000_000_000)
    script_path = own_path.parent / "contracts/unrealistic_staking.py"
    plutus_script = build(script_path, as_ledger_address(staking_user.address))
    register_and_delegate(
        staking_user.signing_key, plutus_script, stake_pool.pool_id, context
    )


def test_register_and_delegate_wrong_order():
    api = MockFrostApi()
    context = MockChainContext(api=api)
    staking_user = MockUser(api)
    stake_pool = MockPool(api)

    staking_user.fund(100_000_000_000)
    script_path = own_path.parent / "contracts/unrealistic_staking.py"
    plutus_script = build(script_path, as_ledger_address(staking_user.address))
    pytest.raises(
        TransactionFailedException,
        register_and_delegate,
        staking_user.signing_key,
        plutus_script,
        stake_pool.pool_id,
        context,
        reverse_cert_order=True,
    )


def test_register_and_delegate_no_script():
    api = MockFrostApi()
    context = MockChainContext(api=api)
    staking_user = MockUser(api)
    stake_pool = MockPool(api)

    staking_user.fund(100_000_000_000)
    script_path = own_path.parent / "contracts/unrealistic_staking.py"
    plutus_script = build(script_path, as_ledger_address(staking_user.address))
    pytest.raises(
        ValueError,
        register_and_delegate,
        staking_user.signing_key,
        plutus_script,
        stake_pool.pool_id,
        context,
        add_certificate_script=False,
    )


@unittest.skip
def test_withdraw():
    api = MockFrostApi()
    context = MockChainContext(api=api)
    staking_user = MockUser(api)
    recipient_user = MockUser(api)
    stake_pool = MockPool(api)

    staking_user.fund(100_000_000_000)

    script_path = own_path.parent / "contracts/unrealistic_staking.py"
    plutus_script = build(script_path, as_ledger_address(recipient_user.address))
    stake_info = register_and_delegate(
        staking_user.signing_key, plutus_script, stake_pool.pool_id, context
    )

    # Withdraw
    withdraw(
        recipient_user.address,
        1000000,
        staking_user.signing_key,
        plutus_script,
        context,
    )


@unittest.skip
def test_withdraw_rewards():
    api = MockFrostApi()
    context = MockChainContext(api=api)
    staking_user = MockUser(api)
    recipient_user = MockUser(api)
    stake_pool = MockPool(api)

    staking_user.fund(100_000_000_000)
    script_path = own_path.parent / "contracts/unrealistic_staking.py"
    plutus_script = build(script_path, as_ledger_address(recipient_user.address))
    stake_info = register_and_delegate(
        staking_user.signing_key, plutus_script, stake_pool.pool_id, context
    )

    api.distribute_rewards(10_000_000_000)

    # Withdraw
    withdraw(
        recipient_user.address,
        60_000_000_000,
        staking_user.signing_key,
        plutus_script,
        context,
    )


def test_withdraw_script_failure():
    api = MockFrostApi()
    context = MockChainContext(api=api)
    staking_user = MockUser(api)
    recipient_user = MockUser(api)
    stake_pool = MockPool(api)

    staking_user.fund(100_000_000_000)

    script_path = own_path.parent / "contracts/unrealistic_staking.py"
    plutus_script = build(script_path, as_ledger_address(recipient_user.address))
    stake_info = register_and_delegate(
        staking_user.signing_key, plutus_script, stake_pool.pool_id, context
    )

    api.distribute_rewards(10_000_000_000)

    # Fails if recipient recieves less than double the reward amount
    pytest.raises(
        TransactionFailedException,
        withdraw,
        recipient_user.address,
        19_000_000_000,
        staking_user.signing_key,
        plutus_script,
        context,
    )


if __name__ == "__main__":
    # test_register_and_delegate()
    # test_register_and_delegate_wrong_order()
    # test_register_and_delegate_no_script()
    # test_withdraw()
    test_withdraw_rewards()
    # test_withdraw_script_failure()
