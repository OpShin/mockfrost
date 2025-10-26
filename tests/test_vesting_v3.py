import datetime
import pathlib
from dataclasses import dataclass
import pycardano
import pytest
from pycardano import TransactionFailedException

from plutus_bench import MockChainContext, MockUser
from plutus_bench.mock import MockFrostApi

from tests.vesting import *
from plutus_bench.tool import address_from_script, load_contract, ScriptType

import time

own_path = pathlib.Path(__file__)


@dataclass()
class VestingDatum(pycardano.PlutusData):
    CONSTR_ID = 0
    beneficiary: bytes
    deadline: int


@dataclass()
class VestingRedeemer(pycardano.PlutusData):
    CONSTR_ID = 0


def test_vesting_v3_script():
    api = MockFrostApi()
    context = MockChainContext(api=api)
    giver = MockUser(api)
    giver.fund(100_000_000)

    taker = MockUser(api)
    taker.fund(5_000_000)  # collateral

    vesting_script = load_contract(
        own_path.parent / "assets/vesting_v3.plutus", ScriptType.PlutusV3
    )
    current_time = int(datetime.datetime.now().timestamp()) # int(datetime.datetime(2020, 1, 1).timestamp())

    datum = VestingDatum(
        beneficiary=bytes(taker.verification_key.hash()),
        deadline=int(current_time) * 1000,  # must be in milliseconds
    )

    give(giver.signing_key, vesting_script, context, 50_000_000, datum)

    redeemer = pycardano.Redeemer(VestingRedeemer())
    param = api.genesis_param
    system_start, slot_length = param.system_start, param.slot_length

    # Valid range will be set to last_block_slot-1000 by default
    # Should fail with offset = 999 and pass with offset 1000
    offset = 999
    api.set_block_slot(int((current_time - system_start) / slot_length) + offset)

    pytest.raises(
        TransactionFailedException,
        take,
        taker.signing_key,
        vesting_script,
        redeemer,
        context,
        25_000_000,
        datum=datum,
    )

    offset = 1000
    api.set_block_slot(int((current_time - system_start) / slot_length) + offset)
    take(taker.signing_key, vesting_script, redeemer, context, 25_000_000, datum=datum)


if __name__ == "__main__":
    test_vesting_v3_script()
