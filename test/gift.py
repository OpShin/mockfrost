import pathlib

import pycardano
from pycardano import ChainContext
from plutus_bench.tool import load_contract, ScriptType, address_from_script

def spend_from_gift_contract(
    payment_key: pycardano.PaymentSigningKey,
    gift_contract_path: str | pathlib.Path,
    context: ChainContext
):
    network = context.network
    gift_contract = load_contract(gift_contract_path, ScriptType.PlutusV2)
    script_hash = pycardano.script_hash(gift_contract)
    script_address = address_from_script(gift_contract, network)
    payment_vkey_hash = payment_key.to_verification_key().hash()
    payment_address = pycardano.Address(payment_part=payment_vkey_hash, network=network)
    utxos = context.utxos(script_address)
    spend_utxo = None
    for u in utxos:
        datum = u.output.datum
        if datum is None:
            continue
        if datum != payment_vkey_hash.payload:
            continue
        spend_utxo = u
        break
    assert spend_utxo is not None, "No UTxO found"

    txbuilder = pycardano.TransactionBuilder(
        context=context,
    )
    txbuilder.add_input_address(payment_address)
    txbuilder.add_script_input(
        spend_utxo,
        gift_contract,
        None,
        pycardano.Redeemer(0),
    )
    tx = txbuilder.build_and_sign(
        signing_keys=[payment_key],
        change_address=payment_address,
    )
    context.submit_tx(tx)


