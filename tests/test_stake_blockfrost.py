import pathlib
import unittest
from multiprocessing import Process
from time import sleep

import pycardano
import pytest
import uvicorn
from pycardano import TransactionFailedException
from starlette.testclient import TestClient

from plutus_bench import MockChainContext, MockUser, MockPool
from plutus_bench.mock import MockFrostApi

from tests.stake import register_and_delegate, withdraw
from pycardano.crypto.bech32 import decode
from opshin import build
from plutus_bench.mockfrost.client import MockFrostClient, MockFrostUser, MockFrostPool
from plutus_bench.mockfrost.server import app

own_path = pathlib.Path(__file__)


def run_server():
    uvicorn.run(app, port=8000)


@pytest.fixture
def server():
    proc = Process(target=run_server, args=(), daemon=True)
    proc.start()
    sleep(1)  # Wait for server to start
    yield
    proc.kill()  # Cleanup after test


def test_register_and_delegate(server):
    client = MockFrostClient(base_url="http://127.0.0.1:8000")
    session = client.create_session()
    context = session.chain_context()
    staking_user = MockFrostUser(session)

    # api = MockFrostApi()
    # context = MockChainContext(api=api)
    # staking_user = MockUser(api)
    # stake_pool = MockPool(api)
    stake_pool = MockFrostPool(session)

    staking_user.fund(100_000_000_000)
    script_path = own_path.parent / "contracts/unrealistic_staking.py"
    plutus_script = build(script_path, bytes(staking_user.verification_key.hash()))
    register_and_delegate(
        staking_user.signing_key, plutus_script, stake_pool.pool_id, context
    )


def test_withdraw(server):
    # api = MockFrostApi()
    # context = MockChainContext(api=api)
    # staking_user = MockUser(api)
    # recipient_user = MockUser(api)
    # stake_pool = MockPool(api)
    client = MockFrostClient(base_url="http://127.0.0.1:8000")
    session = client.create_session()
    context = session.chain_context()
    staking_user = MockFrostUser(session)
    recipient_user = MockFrostUser(session)
    stake_pool = MockFrostPool(session)

    staking_user.fund(100_000_000_000)

    script_path = own_path.parent / "contracts/unrealistic_staking.py"
    plutus_script = build(script_path, bytes(recipient_user.verification_key.hash()))
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

    stake_address = stake_info["stake_address"]
    script_payment_address = stake_info["script_payment_address"]


if __name__ == "__main__":
    test_register_and_delegate()
    test_withdraw()
