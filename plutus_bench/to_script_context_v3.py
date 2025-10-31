import fractions
from collections import defaultdict
from typing import Optional, Tuple, cast

import pycardano

from .ledger.api_v3 import *


def to_staking_credential(
    sk: Union[
        pycardano.VerificationKeyHash,
        pycardano.ScriptHash,
        pycardano.PointerAddress,
        None,
    ],
):
    if sk is None:
        return NoStakingCredential()
    return SomeStakingCredential(to_staking_hash(sk))


def to_staking_hash(
    sk: Union[
        pycardano.VerificationKeyHash, pycardano.ScriptHash, pycardano.PointerAddress
    ],
):
    if isinstance(sk, pycardano.PointerAddress):
        return StakingPtr(sk.slot, sk.tx_index, sk.cert_index)
    if isinstance(sk, (pycardano.VerificationKeyHash, pycardano.ScriptHash)):
        return StakingHash(to_credential(sk))
    raise NotImplementedError(f"Unknown stake key type {type(sk)}")


def to_withdrawal(
    wdrl: Optional[pycardano.Withdrawals],
) -> Dict[StakingCredential, int]:
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
    assert isinstance(vkh, pycardano.VerificationKeyHash)
    return PubKeyHash(vkh.payload)


def to_tx_id(tx_id: pycardano.TransactionId):
    assert isinstance(tx_id, pycardano.TransactionId)
    return TxId(tx_id.payload)


def to_dcert(c: pycardano.Certificate) -> Certificate:
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


def to_script_credential(credential: pycardano.ScriptHash) -> ScriptCredential:
    assert isinstance(credential, pycardano.ScriptHash)
    return ScriptCredential(credential.payload)


def to_address(a: pycardano.Address):
    return Address(
        to_credential(a.payment_part),
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
) -> ScriptPurpose:
    v = r.tag
    if v == pycardano.RedeemerTag.SPEND:
        spent_input = tx_body.inputs[r.index]
        return Spending(to_tx_out_ref(spent_input))
    elif v == pycardano.RedeemerTag.MINT:
        minted_id = sorted(tx_body.mint.data.keys())[r.index]
        return Minting(PolicyId(minted_id.payload))
    elif v == pycardano.RedeemerTag.CERTIFICATE:
        certificate = tx_body.certificates[r.index]
        return Publishing(r.index, to_dcert(certificate))
    elif v == pycardano.RedeemerTag.WITHDRAWAL:
        withdrawal = sorted(tx_body.withdraws.keys())[r.index]
        script_hash = pycardano.Address.from_primitive(withdrawal).staking_part
        return Withdrawing(to_staking_hash(script_hash))
    elif v == pycardano.RedeemerTag.VOTING:
        return Voting(to_voter(sorted(tx_body.voting_procedures.keys())[r.index]))
    elif v == pycardano.RedeemerTag.PROPOSING:
        return Proposing(
            r.index, to_proposal_procedure(sorted(tx_body.proposal_procedures)[r.index])
        )
    else:
        raise NotImplementedError()


def to_credential(
    credential: Union[pycardano.VerificationKeyHash, pycardano.ScriptHash],
) -> Credential:
    if isinstance(credential, pycardano.VerificationKeyHash):
        return PubKeyCredential(credential.payload)
    if isinstance(credential, pycardano.ScriptHash):
        return ScriptCredential(credential.payload)
    raise NotImplementedError(f"Unknown credential type {type(credential)}")


def to_voter(voter: pycardano.Voter) -> Voter:
    if voter.voter_type == pycardano.VoterType.DREP:
        return DelegateRepresentative(
            to_credential(voter.credential),
        )
    elif voter.voter_type == pycardano.VoterType.COMMITTEE_HOT:
        return ConstitutionalCommitteeMember(
            to_credential(voter.credential),
        )
    elif voter.voter_type == pycardano.VoterType.STAKING_POOL:
        return StakePool(
            to_pubkeyhash(voter.credential),
        )
    else:
        raise NotImplementedError(f"Unknown voter type {voter.voter_type}")


def to_gov_action_id(gov_action_id: pycardano.GovActionId) -> GovernanceActionId:
    return GovernanceActionId(
        to_tx_id(gov_action_id.transaction_id),
        gov_action_id.gov_action_index,
    )


def to_votes(
    voting_procedures: Optional[pycardano.VotingProcedures] = None,
) -> Dict[Voter, Dict[GovernanceActionId, Vote]]:
    if voting_procedures is None:
        return {}
    res_dict = defaultdict(dict)
    for voter, gov_actions in voting_procedures.to_shallow_primitive().items():
        for gov_action_id, gov_action in cast(
            gov_actions, pycardano.GovActionIdToVotingProcedure()
        ).items():
            res_dict[to_voter(voter)][to_gov_action_id(gov_action_id)] = gov_action
    return dict(res_dict)


def to_maybe_governance_action_id(
    gov_action_id: Optional[pycardano.GovActionId],
) -> MaybeGovernanceActionId:
    if gov_action_id is None:
        return NoGovernanceActionId()
    return SomeGovernanceActionId(
        to_gov_action_id(gov_action_id),
    )


def to_protocol_parameters_update(
    protocol_parameters: pycardano.ProtocolParamUpdate,
) -> Dict[int, Datum]:
    # TODO
    return {}


def to_maybe_script_credential(
    policy_hash: Optional[pycardano.PolicyHash] = None,
) -> Union[SomeScriptHash, NoScriptHash]:
    if policy_hash is None:
        return NoScriptHash()
    return SomeScriptHash(policy_hash.payload)


def to_evicted_members(
    committee_cold_credentials: pycardano.OrderedSet[pycardano.CommitteeColdCredential],
) -> List[Credential]:
    return [to_credential(x.credential) for x in committee_cold_credentials]


def to_added_members(
    commitee_cold_credential_epoch_map: pycardano.CommitteeColdCredentialEpochMap,
) -> Dict[Credential, int]:
    res = {}
    for cold_credential, epoch in commitee_cold_credential_epoch_map.to_dict().items():
        res[to_credential(cold_credential.credential)] = epoch
    return res


def to_treasury_withdrawals(
    treasury_withdrawals: pycardano.TreasuryWithdrawal,
) -> Dict[Credential, Lovelace]:
    res_dict = {}
    for recipient, amount in treasury_withdrawals.to_shallow_primitive():
        res_dict[to_credential(recipient.credential)] = amount
    return res_dict


def to_fraction(
    fraction: fractions.Fraction,
) -> Fraction:
    return Fraction(
        fraction.numerator,
        fraction.denominator,
    )


def to_anchor(anchor: pycardano.Anchor) -> Anchor:
    return Anchor(
        url=anchor.url.encode("utf8"),
        data_hash=anchor.data_hash.payload,
    )


def to_constitution(
    constitution: Tuple[Anchor, Optional[ScriptHash]],
) -> Constitution:
    return Constitution(
        anchor=to_anchor(constitution[0]),
        guardrails=to_maybe_script_credential(constitution[1]),
    )


def to_gov_action(gov_action: pycardano.GovAction) -> GovernanceAction:
    if isinstance(gov_action, pycardano.ParameterChangeAction):
        return GAParameterChange(
            ancestor=to_maybe_governance_action_id(gov_action.gov_action_id),
            new_parameters=to_protocol_parameters_update(
                gov_action.protocol_param_update
            ),
            guardrails=to_maybe_script_credential(gov_action.policy_hash),
        )
    if isinstance(gov_action, pycardano.HardForkInitiationAction):
        return GAHardForkInitiation(
            ancestor=to_maybe_governance_action_id(gov_action.gov_action_id),
            new_version=ProtocolVersion(
                gov_action.protocol_version.numerator,
                gov_action.protocol_version.denominator,
            ),
        )
    if isinstance(gov_action, pycardano.TreasuryWithdrawalsAction):
        return GATreasuryWithdrawals(
            treasury_withdrawals=to_treasury_withdrawals(gov_action.withdrawals),
            guardrails=to_maybe_script_credential(gov_action.policy_hash),
        )
    if isinstance(gov_action, pycardano.NoConfidence):
        return GANoConfidence(
            ancestor=to_maybe_governance_action_id(gov_action.gov_action_id),
        )
    if isinstance(gov_action, pycardano.UpdateCommittee):
        return GAUpdateCommittee(
            ancestor=to_maybe_governance_action_id(gov_action.gov_action_id),
            evicted_members=to_evicted_members(gov_action.committee_cold_credentials),
            added_members=to_added_members(gov_action.committee_expiration),
            quorum=to_fraction(gov_action.quorum),
        )
    if isinstance(gov_action, pycardano.NewConstitution):
        return GANewConstitution(
            ancestor=to_maybe_governance_action_id(gov_action.gov_action_id),
            constitution=to_constitution(gov_action.constitution),
        )
    if isinstance(gov_action, pycardano.InfoAction):
        return GAInfo()
    raise NotImplementedError(f"Unknown gov_action type {type(gov_action)}")


def to_proposal_procedure(
    proposal_procedure: pycardano.ProposalProcedure,
) -> ProposalProcedure:
    return ProposalProcedure(
        deposit=proposal_procedure.deposit,
        reward_account=to_credential(proposal_procedure.reward_account),
        governance_action=to_gov_action(proposal_procedure.gov_action),
        anchor=to_anchor(proposal_procedure.anchor),
    )


def to_proposal_procedures(
    proposal_procedures: Optional[
        pycardano.NonEmptyOrderedSet[pycardano.ProposalProcedure]
    ],
) -> List[ProposalProcedure]:
    if proposal_procedures is None:
        return []
    res_list = []
    for proposal_procedure in proposal_procedures:
        res_list.append(to_proposal_procedure(proposal_procedure))
    return res_list


def to_optional_lovelace(
    amount: Optional[Lovelace],
) -> OptionalLovelace:
    if amount is None:
        return NoValue()
    return BoxedInt(amount)


def to_tx_info(
    tx: pycardano.Transaction,
    resolved_inputs: List[pycardano.TransactionOutput],
    resolved_reference_inputs: List[pycardano.TransactionOutput],
    posix_from_slot,
):
    tx_body = tx.transaction_body
    datums = [
        pycardano.RawPlutusData(o.datum)
        for o in tx_body.outputs + resolved_inputs + resolved_reference_inputs
        if o.datum is not None
    ]
    if tx.transaction_witness_set.plutus_data:
        datums += [
            pycardano.RawPlutusData(x) for x in tx.transaction_witness_set.plutus_data
        ]

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
        tx_body.fee,
        multiasset_to_value(tx_body.mint),
        [to_dcert(c) for c in tx_body.certificates] if tx_body.certificates else [],
        to_withdrawal(tx_body.withdraws),
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
        to_votes(tx_body.voting_procedures),
        to_proposal_procedures(tx_body.proposal_procedures),
        to_optional_lovelace(tx_body.current_treasury_value),
        to_optional_lovelace(tx_body.donation),
    )


def to_script_context(
    tx_info_args: Tuple[
        pycardano.Transaction,
        List[pycardano.TransactionOutput],
        List[pycardano.TransactionOutput],
        ...,
    ],
    redeemer: pycardano.Redeemer,
):
    return ScriptContext(
        to_tx_info(*tx_info_args),
        redeemer.data,
        to_redeemer_purpose(redeemer, tx_info_args[0].transaction_body),
    )
