import uuid
from dataclasses import dataclass
from typing import Union

import requests
from pycardano.pool_params import PoolId
from pycardano.crypto.bech32 import decode, encode
from pycardano import (
    TransactionOutput,
    UTxO,
    TransactionInput,
    BlockFrostChainContext,
    Network,
    PaymentSigningKey,
    PaymentVerificationKey,
    Address,
    Value,
    StakePoolKeyPair,
    PoolKeyHash,
)
from blockfrost import BlockFrostApi


@dataclass
class MockFrostSession:
    client: "MockFrostClient"
    session_id: str

    def info(self):
        return self.client._get(f"/session/{self.session_id}")

    def delete(self):
        return self.client._del(f"/session/{self.session_id}")

    def add_txout(self, txout: TransactionOutput) -> dict:
        return self.client._post(
            f"/{self.session_id}/ledger/txo", json={"tx_cbor": txout.to_cbor().hex()}
        )

    def del_txout(self, txout: TransactionInput) -> bool:
        return self.client._del(
            f"/{self.session_id}/ledger/txo",
            json={
                "tx_id": txout.transaction_id.payload.hex(),
                "output_index": txout.index,
            },
        )

    def add_utxo(self, utxo: UTxO) -> dict:
        return self.client._put(
            f"/{self.session_id}/ledger/utxo", json={"utxo": utxo.to_cbor().hex()}
        )

    def set_slot(self, slot: int) -> int:
        return self.client._put(f"/{self.session_id}/slot", json={"slot": slot})

    def blockfrost_api(self) -> BlockFrostApi:
        return BlockFrostApi(
            project_id="",
            base_url=self.client.base_url + "/" + self.session_id + "/api",
            api_version="v1",
        )

    def chain_context(self, network=Network.TESTNET):
        return BlockFrostChainContext(
            project_id="",
            network=network,
            base_url=self.client.base_url + "/" + self.session_id + "/api",
        )

    def add_mock_pool(self, pool_id: PoolId) -> str:
        return self.client._put(
            f"/{self.session_id}/pools/pool", json={"pool_id": pool_id.value}
        )

    def distribute_rewards(self, rewards: int) -> int:
        return self.client._put(
            f"/{self.session_id}/pools/distribute", json={"rewards": rewards}
        )


@dataclass
class MockFrostClient:
    base_url: str = "https://mockfrost.dev"
    session: requests.Session = requests.Session()

    def __post_init__(self):
        self.base_url = self.base_url.rstrip("/")

    def _handle_errors(self, response: requests.Response):
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            raise RuntimeError(f"HTTP error: {e}, response: {response.text}")
        except requests.exceptions.JSONDecodeError:
            raise RuntimeError(f"Non-JSON response: {response.text}")

    def _get(self, path: str, **kwargs):
        return self._handle_errors(self.session.get(self.base_url + path, **kwargs))

    def _post(self, path: str, **kwargs):
        return self._handle_errors(self.session.post(self.base_url + path, **kwargs))

    def _put(self, path: str, **kwargs):
        return self._handle_errors(self.session.put(self.base_url + path, **kwargs))

    def _del(self, path: str, **kwargs):
        return self._handle_errors(self.session.delete(self.base_url + path, **kwargs))

    def create_session(
        self, protocol_parameters=None, genesis_parameters=None
    ) -> MockFrostSession:
        session_id = self._post(
            "/session",
            json={
                "protocol_parameters": protocol_parameters,
                "genesis_parameters": genesis_parameters,
            },
        )
        return MockFrostSession(client=self, session_id=session_id)


class MockFrostUser:
    def __init__(self, api: MockFrostSession, network=Network.TESTNET):
        self.network = network
        self.api = api
        self.context = api.chain_context()
        self.signing_key = PaymentSigningKey.generate()
        self.verification_key = PaymentVerificationKey.from_signing_key(
            self.signing_key
        )
        self.address = Address(
            payment_part=self.verification_key.hash(), network=self.network
        )

    def fund(self, amount: Union[int, Value]):
        self.api.add_txout(
            TransactionOutput(self.address, amount),
        )

    def utxos(self):
        return self.context.utxos(self.address)

    def balance(self) -> Value:
        return sum([utxo.output.amount for utxo in self.utxos()], start=Value())


class MockFrostPool:
    def __init__(
        self, api: MockFrostSession, pool_id: PoolId = None, network=Network.TESTNET
    ):
        self.network = network
        self.api = api

        if pool_id is None:
            self.key_pair = StakePoolKeyPair.generate()
            self.pool_key_hash = self.key_pair.verification_key.hash()
            self.pool_id = PoolId(encode("pool", bytes(self.pool_key_hash)))
        else:
            self.pool_id = pool_id
            self.pool_key_hash = PoolKeyHash.from_primitive(
                bytes(decode(self.pool_id.value))
            )
        self.api.add_mock_pool(self.pool_id)
