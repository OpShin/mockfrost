import pathlib
from multiprocessing import Process
from time import sleep
import cbor2

import pycardano
import pytest
import uvicorn
from pycardano import TransactionFailedException
from starlette.testclient import TestClient

from plutus_bench import MockChainContext, MockUser
from plutus_bench.mock import MockFrostApi

from tests.gift import spend_from_gift_contract
from plutus_bench.tool import address_from_script, load_contract, ScriptType
from plutus_bench.mockfrost.client import (
    MockFrostClient,
    MockFrostUser,
    MockfrostApiError,
)
from plutus_bench.mockfrost.server import app
from blockfrost.utils import ApiError

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


def test_rate_limiting_requests(server):
    client = MockFrostClient(base_url="http://127.0.0.1:8000")
    session = client.create_session()
    context = session.chain_context()
    for i in range(3600):
        context.last_block_slot
    with pytest.raises(ApiError, match="429"):
        print(context.last_block_slot)


def test_rate_limiting_sessions(server):
    client = MockFrostClient(base_url="http://127.0.0.1:8000")
    for i in range(1000):
        session = client.create_session()
    with pytest.raises(MockfrostApiError, match="Too many requests"):
        session = client.create_session()


def test_max_transaction_size(server):
    client = MockFrostClient(base_url="http://127.0.0.1:8000")
    session = client.create_session()
    context = session.chain_context()
    payment_key = MockFrostUser(session)
    payment_key.fund(100_000_000)
    # This should exceed max contract size
    gift_contract = pycardano.PlutusV2Script(b"0" * context.protocol_param.max_tx_size)
    gift_address = address_from_script(gift_contract, network=context.network)
    session.add_txout(
        pycardano.TransactionOutput(
            address=gift_address,
            amount=pycardano.Value(coin=1000000),
            datum=payment_key.verification_key.hash().payload,
        ),
    )

    payment_vkey_hash = payment_key.verification_key.hash()
    utxos = context.utxos(gift_address)
    spend_utxo = None
    for u in utxos:
        datum = cbor2.loads(u.output.datum.cbor)
        if datum != payment_vkey_hash.payload:
            continue
        spend_utxo = u
        break
    assert spend_utxo is not None, "No UTxO found"

    txbuilder = pycardano.TransactionBuilder(context=context)
    txbuilder.add_input_address(payment_key.address)
    txbuilder.add_script_input(spend_utxo, gift_contract, None, pycardano.Redeemer(0))
    with pytest.raises(
        pycardano.exception.InvalidTransactionException, match="Transaction size"
    ):
        tx = txbuilder.build_and_sign(
            signing_keys=[payment_key.signing_key],
            change_address=payment_key.address,
            auto_required_signers=True,
        )
        context.submit_tx(tx)


def test_max_resource_limits(server):
    client = MockFrostClient(base_url="http://127.0.0.1:8000")
    session = client.create_session()
    context = session.chain_context()
    payment_key = MockFrostUser(session)
    payment_key.fund(100_000_000)
    # This should exceed max contract size
    gift_contract_path = own_path.parent / "build" / "gift" / "script.plutus"
    gift_contract = load_contract(gift_contract_path, ScriptType.PlutusV2)
    gift_address = address_from_script(gift_contract, network=context.network)
    session.add_txout(
        pycardano.TransactionOutput(
            address=gift_address,
            amount=pycardano.Value(coin=1000000),
            datum=payment_key.verification_key.hash().payload,
        ),
    )

    payment_vkey_hash = payment_key.verification_key.hash()
    utxos = context.utxos(gift_address)
    spend_utxo = None
    for u in utxos:
        datum = cbor2.loads(u.output.datum.cbor)
        if datum != payment_vkey_hash.payload:
            continue
        spend_utxo = u
        break
    assert spend_utxo is not None, "No UTxO found"

    txbuilder = pycardano.TransactionBuilder(context=context)
    txbuilder.add_input_address(payment_key.address)
    txbuilder.add_script_input(spend_utxo, gift_contract, None, pycardano.Redeemer(0))
    tx = txbuilder.build_and_sign(
        signing_keys=[payment_key.signing_key],
        change_address=payment_key.address,
        auto_required_signers=True,
    )
    redeemers = tx.transaction_witness_set.redeemer
    redeemer_key = next(iter(redeemers))
    redeemer_value = redeemers[redeemer_key]
    old_steps = redeemer_value.ex_units.steps
    redeemer_value.ex_units.steps = context.protocol_param.max_tx_ex_steps + 1

    with pytest.raises(
        pycardano.exception.TransactionFailedException, match="Invalid ExUnits:"
    ):
        context.submit_tx(tx)

    redeemer_value.ex_units.steps = old_steps
    redeemer_value.ex_units.mem = context.protocol_param.max_tx_ex_mem + 1
    with pytest.raises(
        pycardano.exception.TransactionFailedException, match="Invalid ExUnits:"
    ):
        context.submit_tx(tx)


if __name__ == "__main__":
    # proc = Process(target = run_server, args=(), daemon=True)
    # proc.start()
    # test_max_transaction_size_
    # test_rate_limiting_requests(False)
    test_max_resource_limits(False)
    # proc.kill()
