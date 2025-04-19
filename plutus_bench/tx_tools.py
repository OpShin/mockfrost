from functools import lru_cache
from typing import Optional, Union, List
from dataclasses import dataclass

import cbor2
import pycardano
import uplc
import uplc.cost_model
import uplc.ast
from pycardano import (
    ScriptHash,
    RedeemerTag,
    plutus_script_hash,
    datum_hash,
    PlutusV1Script,
    PlutusV2Script,
    UTxO,
)

from pycardano import Datum as Anything, PlutusData

# from .ledger.api_v2 import *
from .ledger.api_v1 import ScriptContext as ScriptContextV1
from .ledger.api_v2 import ScriptContext as ScriptContextV2
from .to_script_context_v2 import (
    to_spending_script_context as to_spending_script_context_v2,
    to_minting_script_context as to_minting_script_context_v2,
    to_certificate_script_context as to_certificate_script_context_v2,
    to_withdrawal_script_context as to_withdrawal_script_context_v2,
)
from .to_script_context_v1 import (
    to_spending_script_context as to_spending_script_context_v1,
    to_minting_script_context as to_minting_script_context_v1,
    to_certificate_script_context as to_certificate_script_context_v1,
    to_withdrawal_script_context as to_withdrawal_script_context_v1,
)

from .tool import ScriptType


@dataclass
class ScriptInvocation:
    script: pycardano.ScriptType
    datum: Optional[pycardano.Datum]
    redeemer: Union[pycardano.Redeemer, pycardano.RedeemerMap]
    script_context: Union[ScriptContextV1, ScriptContextV2]


def generate_script_contexts(tx_builder: pycardano.TransactionBuilder):
    """Generates for each evaluated script, with which parameters it should be called"""
    # TODO this only handles PlutusV2, no other script contexts are currently supported

    tx = tx_builder._build_full_fake_tx()
    # we assume that reference inputs are UTxO objects!
    input_to_resolved_output = {}
    for utxo in tx_builder.inputs + list(tx_builder.reference_inputs):
        assert isinstance(utxo, pycardano.UTxO)
        input_to_resolved_output[utxo.input] = utxo.output
    # input_to_resolved_output = {
    #     utxo.input: utxo.output
    #     for utxo in tx_builder.inputs + tx_builder.reference_inputs
    # }
    resolved_inputs = [
        UTxO(i, input_to_resolved_output[i]) for i in tx.transaction_body.inputs
    ]
    resolved_reference_inputs = [
        UTxO(i, input_to_resolved_output[i])
        for i in tx.transaction_body.reference_inputs
    ]
    return generate_script_contexts_resolved(
        tx, resolved_inputs, resolved_reference_inputs
    )


def as_redeemer(
    r: Union[pycardano.Redeemer, pycardano.RedeemerKey], redeemers: pycardano.Redeemers
):
    if isinstance(r, pycardano.RedeemerKey):
        v = redeemers[r]
        new_r = pycardano.Redeemer(data=v.data, ex_units=v.ex_units)
        new_r.tag = r.tag
        new_r.index = r.index
        return new_r
    else:
        return r


def generate_script_contexts_resolved(
    tx: pycardano.Transaction,
    resolved_inputs: List[UTxO],
    resolved_reference_inputs: List[UTxO],
    posix_from_slot,
):
    tx_info_args = (
        tx,
        [i.output for i in resolved_inputs],
        [i.output for i in resolved_reference_inputs],
        posix_from_slot,
    )
    datum = None
    script_contexts = []
    for i, spending_input in enumerate(resolved_inputs):
        if not isinstance(spending_input.output.address.payment_part, ScriptHash):
            continue
        try:
            # Redeemers is Union[RedeemerMap, List[Redeemer]]
            spending_redeemer = as_redeemer(
                next(
                    r
                    for r in tx.transaction_witness_set.redeemer
                    if r.index == i and r.tag == RedeemerTag.SPEND
                ),
                tx.transaction_witness_set.redeemer,
            )
        except (StopIteration, TypeError):
            raise ValueError(
                f"Missing redeemer for script input {i} (index or tag set incorrectly or missing redeemer)"
            )
        script_type = None
        try:
            spending_script = next(
                s
                for s in tx.transaction_witness_set.plutus_v2_script
                if plutus_script_hash(PlutusV2Script(s))
                == spending_input.output.address.payment_part
            )
            script_type = ScriptType.PlutusV2

        except (StopIteration, TypeError):
            try:
                spending_script = next(
                    s
                    for s in tx.transaction_witness_set.plutus_v1_script
                    if plutus_script_hash(PlutusV1Script(s))
                    == spending_input.output.address.payment_part
                )
                script_type = ScriptType.PlutusV1
            except Exception as e:
                raise NotImplementedError(
                    f"Can not validate spending of non plutus v1 or v2 script (or plutus v1 or v2 script is not in context)"
                )
        if spending_input.output.datum is not None:
            assert (
                script_type != ScriptType.PlutusV1
            ), "Only datum hash is supported for plutus v1 scripts"
            datum = spending_input.output.datum
        elif spending_input.output.datum_hash is not None:
            datum_h = spending_input.output.datum_hash
            try:
                datum = next(
                    d
                    for d in tx.transaction_witness_set.plutus_data or []
                    if datum_hash(d) == datum_h
                )
            except StopIteration:
                raise ValueError(
                    f"No datum with hash '{datum_h.payload.hex()}' provided for transaction"
                )
        else:
            raise ValueError(
                "Spending input is missing an attached datum and can not be spent"
            )

        if script_type is ScriptType.PlutusV1:
            script_context = to_spending_script_context_v1(
                tx_info_args, spending_input.input
            )
        elif script_type is ScriptType.PlutusV2:
            script_context = to_spending_script_context_v2(
                tx_info_args, spending_input.input
            )
        else:
            raise NotImplementedError()

        script_contexts.append(
            ScriptInvocation(
                spending_script,
                datum,
                spending_redeemer,
                script_context,
                # ScriptContext(tx_info, Spending(to_tx_out_ref(spending_input.input))),
            )
        )
    for i, minting_script_hash in enumerate(tx.transaction_body.mint or []):
        try:
            minting_redeemer = as_redeemer(
                next(
                    r
                    for r in tx.transaction_witness_set.redeemer
                    if r.index == i and r.tag == RedeemerTag.MINT
                ),
                tx.transaction_witness_set.redeemer,
            )
        except StopIteration:
            raise ValueError(
                f"Missing redeemer for mint {i} (index or tag set incorrectly or missing redeemer)"
            )
        minting_script, script_type = next(
            (
                (s, ScriptType.PlutusV1)
                for s in tx.transaction_witness_set.plutus_v1_script or []
                if plutus_script_hash(PlutusV2Script(s)) == minting_script_hash
            ),
            (None, None),
        )
        if not minting_script:
            minting_script, script_type = (
                next(
                    (
                        (s, ScriptType.PlutusV2)
                        for s in tx.transaction_witness_set.plutus_v2_script or []
                        if plutus_script_hash(PlutusV2Script(s)) == minting_script_hash
                    ),
                    (minting_script, script_type),
                )
                if not minting_script
                else minting_script
            )

        assert (
            minting_script and script_type
        ), f"Can not validate spending of non plutus v1 or v2 scripts (or plutus v1 or v2 script is not in context)"

        if script_type == ScriptType.PlutusV1:
            script_context = to_minting_script_context_v1(tx_info_args, minting_script)
        elif script_type == ScriptType.PlutusV2:
            script_context = to_minting_script_context_v2(tx_info_args, minting_script)
        else:
            raise NotImplementedError()

        script_contexts.append(
            ScriptInvocation(minting_script, datum, minting_redeemer, script_context)
        )
    for i, certificate in enumerate(tx.transaction_body.certificates or []):
        try:
            certificate_redeemer = as_redeemer(
                next(
                    r
                    for r in tx.transaction_witness_set.redeemer or []
                    if r.index == i and r.tag == RedeemerTag.CERTIFICATE
                ),
                tx.transaction_witness_set.redeemer,
            )
        except StopIteration:
            if isinstance(certificate, pycardano.StakeRegistration):
                #  TODO: Check can this always be skipped?
                continue
            raise ValueError(
                f"Missing redeemer for certificate {i} (index or tag set incorrectly or missing redeemer)"
            )

        certificate_script, script_type = next(
            (
                (s, ScriptType.PlutusV1)
                for s in tx.transaction_witness_set.plutus_v1_script or []
                if plutus_script_hash(PlutusV1Script(s))
                == certificate.stake_credential.credential
            ),
            (None, None),
        )
        certificate_script, script_type = next(
            (
                (s, ScriptType.PlutusV2)
                for s in tx.transaction_witness_set.plutus_v2_script or []
                if plutus_script_hash(PlutusV2Script(s))
                == certificate.stake_credential.credential
            ),
            (certificate_script, script_type),
        )
        assert (
            certificate_script and script_type
        ), "Can not validate spending of non plutus v1 or v2 scripts (or plutus v1 or v2 script is not in context)"

        if script_type == ScriptType.PlutusV1:
            script_context = to_certificate_script_context_v1(tx_info_args, certificate)
        elif script_type == ScriptType.PlutusV2:
            script_context = to_certificate_script_context_v2(tx_info_args, certificate)
        else:
            raise NotImplementedError()

        script_contexts.append(
            ScriptInvocation(
                certificate_script, datum, certificate_redeemer, script_context
            )
        )
    for i, address in enumerate(sorted(tx.transaction_body.withdraws or {})):
        try:
            withdrawal_redeemer = as_redeemer(
                next(
                    r
                    for r in tx.transaction_witness_set.redeemer
                    if r.index == i and r.tag == RedeemerTag.WITHDRAWAL
                ),
                tx.transaction_witness_set.redeemer,
            )
        except StopIteration:
            raise ValueError(
                f"Missing redeemer for withdrawal {i} (index or tag set incorrectly or missing redeemer)"
            )
        script_hash = pycardano.Address.from_primitive(address).staking_part
        withdrawal_script, script_type = next(
            (
                (s, ScriptType.PlutusV1)
                for s in tx.transaction_witness_set.plutus_v1_script or []
                if plutus_script_hash(PlutusV1Script(s)) == script_hash
            ),
            (None, None),
        )
        withdrawal_script, script_type = next(
            (
                (s, ScriptType.PlutusV2)
                for s in tx.transaction_witness_set.plutus_v2_script or []
                if plutus_script_hash(PlutusV2Script(s)) == script_hash
            ),
            (withdrawal_script, script_type),
        )
        assert (
            withdrawal_script and script_type
        ), "Can not validate spending of non plutus v1 or v2 scripts (or plutus v1 or v2 script is not in context)"

        if script_type == ScriptType.PlutusV1:
            script_context = to_withdrawal_script_context_v1(tx_info_args, script_hash)
        elif script_type == ScriptType.PlutusV2:
            script_context = to_withdrawal_script_context_v2(tx_info_args, script_hash)
        else:
            raise NotImplementedError("Only Plutus V1 and V2 scripts are supported.")

        script_contexts.append(
            ScriptInvocation(
                withdrawal_script, datum, withdrawal_redeemer, script_context
            )
        )

    return script_contexts


@lru_cache(maxsize = 1000)
def uplc_unflat(script: bytes):
    return uplc.unflatten(script)


def uplc_plutus_data(a: pycardano.Datum) -> PlutusData:
    return uplc.ast.data_from_cbor(cbor2.dumps(a, default=pycardano.default_encoder))


def evaluate_script(script_invocation: ScriptInvocation):
    uplc_program = uplc_unflat(script_invocation.script)
    args = [script_invocation.redeemer.data, script_invocation.script_context]
    if script_invocation.datum is not None:
        args.insert(0, script_invocation.datum)
    args = [uplc_plutus_data(a) for a in args]
    allowed_cpu_steps = script_invocation.redeemer.ex_units.steps
    allowed_mem_steps = script_invocation.redeemer.ex_units.mem
    res = uplc.eval(
        uplc.tools.apply(uplc_program, *args),
        budget=uplc.cost_model.Budget(allowed_cpu_steps, allowed_mem_steps),
    )
    logs = res.logs
    return (
        (res.result),
        (
            res.cost.cpu,
            res.cost.memory,
        ),
        logs,
    )
