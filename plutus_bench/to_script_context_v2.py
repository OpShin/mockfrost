from typing import Optional, Tuple

import pycardano
from pycardano import RawPlutusData

from .ledger.api_v2 import *


def to_staking_credential(
    sk: Union[
        pycardano.VerificationKeyHash,
        pycardano.ScriptHash,
        pycardano.PointerAddress,
        None,
    ],
):
    try:
        return SomeStakingCredential(to_staking_hash(sk))
    except NotImplementedError:
        return NoStakingCredential()


def to_staking_hash(
    sk: Union[
        pycardano.VerificationKeyHash, pycardano.ScriptHash, pycardano.PointerAddress
    ],
):
    if isinstance(sk, pycardano.PointerAddress):
        return StakingPtr(sk.slot, sk.tx_index, sk.cert_index)
    if isinstance(sk, pycardano.VerificationKeyHash):
        return StakingHash(PubKeyCredential(sk.payload))
    if isinstance(sk, pycardano.ScriptHash):
        return StakingHash(ScriptCredential(sk.payload))
    raise NotImplementedError(f"Unknown stake key type {type(sk)}")


def to_wdrl(wdrl: Optional[pycardano.Withdrawals]) -> Dict[StakingCredential, int]:
    if wdrl is None:
        return {}

    def m(k: bytes):
        sk = pycardano.Address.from_primitive(k).staking_part
        return to_staking_hash(sk)

    return {m(key): val for key, val in wdrl.to_primitive().items()}


def to_valid_range(validity_start: Optional[int], ttl: Optional[int], posix_from_slot):
    if validity_start is None:
        lower_bound = LowerBoundPOSIXTime(NegInfPOSIXTime(), FalseData())
    else:
        start = posix_from_slot(validity_start) * 1000
        lower_bound = LowerBoundPOSIXTime(FinitePOSIXTime(start), TrueData())
    if ttl is None:
        upper_bound = UpperBoundPOSIXTime(PosInfPOSIXTime(), FalseData())
    else:
        end = posix_from_slot(ttl) * 1000
        upper_bound = UpperBoundPOSIXTime(FinitePOSIXTime(end), TrueData())
    return POSIXTimeRange(lower_bound, upper_bound)


def to_pubkeyhash(vkh: pycardano.VerificationKeyHash):
    return PubKeyHash(vkh.payload)


def to_tx_id(tx_id: pycardano.TransactionId):
    return TxId(tx_id.payload)


def to_dcert(c: pycardano.Certificate) -> DCert:
    if isinstance(c, pycardano.StakeRegistration):
        return DCertDelegRegKey(to_staking_hash(c.stake_credential.credential))
    elif isinstance(c, pycardano.StakeDelegation):
        return DCertDelegDelegate(
            to_staking_hash(c.stake_credential.credential),
            PubKeyHash(c.pool_keyhash.payload),
        )
    elif isinstance(c, pycardano.StakeDeregistration):
        # TODO
        raise NotImplementedError(
            f"Certificates of type {type(c)} can not be converted yet"
        )
    elif isinstance(c, pycardano.PoolRegistration):
        # TODO
        raise NotImplementedError(
            f"Certificates of type {type(c)} can not be converted yet"
        )
    elif isinstance(c, pycardano.PoolRetirement):
        # TODO
        raise NotImplementedError(
            f"Certificates of type {type(c)} can not be converted yet"
        )
    raise NotImplementedError(f"Certificates of type {type(c)} are not implemented")


def multiasset_to_value(ma: pycardano.MultiAsset) -> Value:
    if ma is None:
        return {b"": {b"": 0}}
    return {
        PolicyId(policy_id): {
            TokenName(asset_name): quantity for asset_name, quantity in asset.items()
        }
        for policy_id, asset in ma.to_shallow_primitive().items()
    }


def value_to_value(v: pycardano.Value):
    ma = multiasset_to_value(v.multi_asset)
    ma[b""] = {b"": v.coin}
    return ma


def to_payment_credential(
    c: Union[pycardano.VerificationKeyHash, pycardano.ScriptHash],
):
    if isinstance(c, pycardano.VerificationKeyHash):
        return PubKeyCredential(PubKeyHash(c.payload))
    if isinstance(c, pycardano.ScriptHash):
        return ScriptCredential(ValidatorHash(c.payload))
    raise NotImplementedError(f"Unknown payment key type {type(c)}")


def to_address(a: pycardano.Address):
    return Address(
        to_payment_credential(a.payment_part),
        to_staking_credential(a.staking_part),
    )


def to_tx_out(o: pycardano.TransactionOutput):
    if o.datum is not None:
        output_datum = SomeOutputDatum(o.datum)
    elif o.datum_hash is not None:
        output_datum = SomeOutputDatumHash(o.datum_hash.payload)
    else:
        output_datum = NoOutputDatum()
    if o.script is None:
        script = NoScriptHash()
    else:
        script = SomeScriptHash(pycardano.script_hash(o.script).payload)
    return TxOut(
        to_address(o.address),
        value_to_value(o.amount),
        output_datum,
        script,
    )


def to_tx_out_ref(i: pycardano.TransactionInput):
    return TxOutRef(
        TxId(i.transaction_id.payload),
        i.index,
    )


def to_tx_in_info(i: pycardano.TransactionInput, o: pycardano.TransactionOutput):
    return TxInInfo(
        to_tx_out_ref(i),
        to_tx_out(o),
    )


def to_redeemer_purpose(
    r: Union[pycardano.RedeemerKey, pycardano.Redeemer],
    tx_body: pycardano.TransactionBody,
):
    v = r.tag
    if v == pycardano.RedeemerTag.SPEND:
        spent_input = tx_body.inputs[r.index]
        return Spending(to_tx_out_ref(spent_input))
    elif v == pycardano.RedeemerTag.MINT:
        minted_id = sorted(tx_body.mint.data.keys())[r.index]
        return Minting(PolicyId(minted_id.payload))
    elif v == pycardano.RedeemerTag.CERTIFICATE:
        certificate = tx_body.certificates[r.index]
        return Certifying(to_dcert(certificate))
    elif v == pycardano.RedeemerTag.WITHDRAWAL:
        withdrawal = sorted(tx_body.withdraws.keys())[r.index]
        script_hash = pycardano.Address.from_primitive(withdrawal).staking_part
        return Rewarding(to_staking_hash(script_hash))
    else:
        raise NotImplementedError()


def to_tx_info(
    tx: pycardano.Transaction,
    resolved_inputs: List[pycardano.TransactionOutput],
    resolved_reference_inputs: List[pycardano.TransactionOutput],
    posix_from_slot,
):
    tx_body = tx.transaction_body
    datums = [
        RawPlutusData(o.datum)
        for o in tx_body.outputs + resolved_inputs + resolved_reference_inputs
        if o.datum is not None
    ]
    if tx.transaction_witness_set.plutus_data:
        datums += [RawPlutusData(x) for x in tx.transaction_witness_set.plutus_data]

    redeemers = (
        tx.transaction_witness_set.redeemer
        if tx.transaction_witness_set.redeemer
        else []
    )
    return TxInfo(
        [to_tx_in_info(i, o) for i, o in zip(tx_body.inputs, resolved_inputs)],
        (
            [
                to_tx_in_info(i, o)
                for i, o in zip(tx_body.reference_inputs, resolved_reference_inputs)
            ]
            if tx_body.reference_inputs is not None
            else []
        ),
        [to_tx_out(o) for o in tx_body.outputs],
        value_to_value(pycardano.Value(tx_body.fee)),
        multiasset_to_value(tx_body.mint),
        [to_dcert(c) for c in tx_body.certificates] if tx_body.certificates else [],
        to_wdrl(tx_body.withdraws),
        to_valid_range(tx_body.validity_start, tx_body.ttl, posix_from_slot),
        (
            [to_pubkeyhash(s) for s in tx_body.required_signers]
            if tx_body.required_signers
            else []
        ),
        (
            {to_redeemer_purpose(k, tx_body): v.data for k, v in redeemers.items()}
            if isinstance(redeemers, pycardano.RedeemerMap)
            else {to_redeemer_purpose(r, tx_body): r.data for r in redeemers}
        ),
        {pycardano.datum_hash(d).payload: d for d in datums},
        to_tx_id(tx_body.id),
    )


def to_spending_script_context(
    tx_info_args: Tuple, spending_input: pycardano.TransactionInput
):
    return ScriptContext(
        to_tx_info(*tx_info_args), Spending(to_tx_out_ref(spending_input))
    )


def to_minting_script_context(
    tx_info_args: Tuple, minting_script: pycardano.PlutusV2Script
):
    return ScriptContext(
        to_tx_info(*tx_info_args),
        Minting(pycardano.script_hash(minting_script).payload),
    )


def to_certificate_script_context(tx_info_args, certificate):
    return ScriptContext(to_tx_info(*tx_info_args), Certifying(to_dcert(certificate)))


def to_withdrawal_script_context(tx_info_args, script_hash):
    return ScriptContext(
        to_tx_info(*tx_info_args), Rewarding(to_staking_hash(script_hash))
    )
