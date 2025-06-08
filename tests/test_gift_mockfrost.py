import pathlib
from multiprocessing import Process
from time import sleep

import pycardano
import pytest
import uvicorn
from pycardano import TransactionFailedException
from starlette.testclient import TestClient

from plutus_bench import MockChainContext, MockUser
from plutus_bench.mock import MockFrostApi

from tests.gift import spend_from_gift_contract
from plutus_bench.tool import address_from_script, load_contract, ScriptType
from plutus_bench.mockfrost.client import MockFrostClient, MockFrostUser
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


def test_spend_from_gift_contract(server):
    client = MockFrostClient(base_url="http://127.0.0.1:8000")
    session = client.create_session()
    context = session.chain_context()
    payment_key = MockFrostUser(session)
    payment_key.fund(100_000_000)
    gift_contract_path = own_path.parent / "build" / "gift" / "script.plutus"
    gift_address = address_from_script(
        load_contract(gift_contract_path, ScriptType.PlutusV2), network=context.network
    )
    session.add_txout(
        pycardano.TransactionOutput(
            address=gift_address,
            amount=pycardano.Value(coin=1000000),
            datum=payment_key.verification_key.hash().payload,
        ),
    )
    spend_from_gift_contract(payment_key.signing_key, gift_contract_path, context)


def test_other_user_spend_from_gift_contract(server):
    client = MockFrostClient(base_url="http://127.0.0.1:8000")
    session = client.create_session()
    context = session.chain_context()
    payment_key = MockFrostUser(session)
    payment_key.fund(100_000_000)

    owning_user = MockFrostUser(session)
    gift_contract_path = own_path.parent / "build" / "gift" / "script.plutus"
    gift_address = address_from_script(
        load_contract(gift_contract_path, ScriptType.PlutusV2), network=context.network
    )
    session.add_txout(
        pycardano.TransactionOutput(
            address=gift_address,
            amount=pycardano.Value(coin=1000000),
            datum=owning_user.verification_key.hash().payload,
        ),
    )
    pytest.raises(
        TransactionFailedException,
        spend_from_gift_contract,
        payment_key.signing_key,
        gift_contract_path,
        context,
        enforce_true_owner=False,
        set_required_signers=True,
    )
    pytest.raises(
        TransactionFailedException,
        spend_from_gift_contract,
        payment_key.signing_key,
        gift_contract_path,
        context,
        enforce_true_owner=False,
        set_required_signers=False,
    )


if __name__ == "__main__":
    # proc = Process(target = run_server, args=(), daemon=True)
    # proc.start()
    test_spend_from_gift_contract(False)
    test_other_user_spend_from_gift_contract(False)

    # proc.kill()
