import pathlib

import pycardano
import pytest
from pycardano import TransactionFailedException

from plutus_bench import MockChainContext, MockUser
from plutus_bench.mock import MockFrostApi

from tests.gift import spend_from_gift_contract
from plutus_bench.tool import address_from_script, load_contract, ScriptType

own_path = pathlib.Path(__file__)


def test_spend_from_gift_contract():
    api = MockFrostApi()
    context = MockChainContext(api=api)
    payment_key = MockUser(api)
    payment_key.fund(100_000_000)
    gift_contract_path = own_path.parent / "assets/gift.plutus"
    gift_address = address_from_script(
        load_contract(gift_contract_path, ScriptType.PlutusV3), network=context.network
    )
    api.add_txout(
        pycardano.TransactionOutput(
            address=gift_address,
            amount=pycardano.Value(coin=1000000),
            datum=payment_key.verification_key.hash().payload,
        ),
    )
    spend_from_gift_contract(payment_key.signing_key, gift_contract_path, context)


def test_other_user_spend_from_gift_contract():
    api = MockFrostApi()
    context = MockChainContext(api)
    payment_key = MockUser(api)
    payment_key.fund(100_000_000)

    owning_user = MockUser(api)
    gift_contract_path = own_path.parent / "build" / "gift" / "script.plutus"
    gift_address = address_from_script(
        load_contract(gift_contract_path, ScriptType.PlutusV3), network=context.network
    )
    api.add_txout(
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
    test_spend_from_gift_contract()
    test_other_user_spend_from_gift_contract()
