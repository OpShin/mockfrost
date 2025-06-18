import os
import dataclasses
import datetime
import tempfile
import uuid
from types import MappingProxyType

import fastapi
from contextlib import asynccontextmanager
import asyncio
import frozendict
from typing import Dict, Optional, Annotated, Union
from multiprocessing import Manager
from collections import OrderedDict
import functools

import pycardano
from fastapi import FastAPI, Body, Request, HTTPException
from fastapi.responses import JSONResponse
from pycardano import (
    ProtocolParameters,
    GenesisParameters,
    TransactionInput,
    TransactionId,
)
from pydantic import BaseModel
import sqlite3
import pickle

from plutus_bench.mock import MockFrostApi, InvalidTransactionError
from plutus_bench.protocol_params import (
    DEFAULT_PROTOCOL_PARAMETERS,
    DEFAULT_GENESIS_PARAMETERS,
)

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware
from slowapi.errors import RateLimitExceeded

from dotenv import load_dotenv

load_dotenv()


def deep_freeze(obj):
    if isinstance(obj, dict):
        return MappingProxyType({k: deep_freeze(v) for k, v in obj.items()})
    elif isinstance(obj, list):
        return tuple(deep_freeze(i) for i in obj)
    elif isinstance(obj, set):
        return frozenset(deep_freeze(i) for i in obj)
    return obj


class ModifiableChainstate:
    def __init__(
        self,
        session_id: uuid.UUID,
        session: "Session",
        chain_state: MockFrostApi,
        session_manager=None,
    ):
        self.session_manager = (
            session_manager if session_manager is not None else SessionManager()
        )
        self.session_id = session_id
        self.session = session
        assert not isinstance(chain_state, ModifiableChainstate)
        self.chain_state = chain_state

    def __getattr__(self, name):
        attr = getattr(self.chain_state, name)
        modifying_func = {
            "set_block_slot",
            "add_utxo",
            "add_txout",
            "remove_txi",
            "remove_utxo",
            "submit_tx",
            "submit_tx_mock",
            "submit_tx_cbor",
            "wait",
            "add_mock_pool",
            "distribute_rewards",
            "transaction_submit_raw",
            "transaction_submit",
        }
        if callable(attr) and name in modifying_func:

            @functools.wraps(attr)
            def wrapped(*args, **kwargs):
                result = attr(*args, **kwargs)
                self.session_manager[self.session_id] = self.session
                return result

            return wrapped
        else:
            return deep_freeze(attr)

    def __setattr__(self, name, value):
        if name not in {"session_manager", "session_id", "session", "chain_state"}:
            self.chain_state.__setattr__(name, value)
            self.session_manager[self.session_id] = self.session
        else:
            super().__setattr__(name, value)


@dataclasses.dataclass
class Session:
    chain_state: Union[MockFrostApi, ModifiableChainstate]
    creation_time: datetime.datetime
    last_access_time: datetime.datetime
    last_modify_time: datetime.datetime


SERVER_CONFIG = {
    "max_session_lifespan": datetime.timedelta(
        hours=int(os.getenv("MOCKFROST_MAX_SESSION_LIFESPAN", 24))
    ),
    "max_idle_time": datetime.timedelta(
        hours=int(os.getenv("MOCKFROST_MAX_SESSION_IDLE_TIME", 1))
    ),
    "database_name": os.getenv("MOCKFROST_SESSION_DATABASE_NAME", "SESSIONS.db"),
    "default_request_limit": os.getenv("MOCKFROST_IP_REQUEST_LIMIT", "3600/hour"),
    "global_shared_session_limit": os.getenv(
        "MOCKFROST_SHARED_SESSION_LIMIT", "24000/day"
    ),
    "ip_session_limit": os.getenv("MOCKFROST_IP_SESSION_LIMIT", "1000/day"),
    "ip_transaction_limit": os.getenv("MOCKFROST_IP_TRANSACTION_LIMIT", "1000/hour"),
}


class SessionManager:
    SESSIONS_CACHE = OrderedDict()

    def __init__(self, prefix="Session:", database_name=SERVER_CONFIG["database_name"]):
        self.conn = sqlite3.connect(database_name)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.cursor = self.conn.cursor()
        self.cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS sessions (
            key TEXT PRIMARY KEY,
            session BLOB NOT NULL,
            creation_time TEXT NOT NULL,
            last_access_time TEXT NOT NULL,
            last_modify_time TEXT NOT NULL
        )
        """
        )
        self.conn.commit()

        self.prefix = prefix

    def key(self, key: uuid.UUID) -> str:
        return self.prefix + str(key)

    def unkey(self, full_key: str) -> uuid.UUID:
        return uuid.UUID(full_key.removeprefix(self.prefix))

    def cleanup(self) -> datetime.datetime:
        """
        Clean up timed out sessions
        Return time of next expiring session
        """
        now = datetime.datetime.utcnow()
        next_session = now + datetime.timedelta(seconds=600)
        for key in self:
            self.cursor.execute(
                "SELECT last_access_time, creation_time FROM sessions WHERE key = ?",
                (self.key(key),),
            )
            last_access_time, creation_time = (
                datetime.datetime.fromisoformat(d) for d in self.cursor.fetchone()
            )
            last_access_expire = last_access_time + SERVER_CONFIG["max_idle_time"]
            creation_expire = creation_time + SERVER_CONFIG["max_session_lifespan"]
            if now >= last_access_expire or now >= creation_expire:
                print(
                    f"Session {key} has expired (last accessed: {last_access_time}, created: {creation_time})"
                )
                del self[key]
                self.SESSIONS_CACHE.pop(key, None)
            else:
                next_session = min(next_session, last_access_expire, creation_expire)
        return next_session

    def __setitem__(self, key: uuid.UUID, value: Session):
        if isinstance(value.chain_state, ModifiableChainstate):
            value.chain_state = value.chain_state.chain_state
        value.last_modify_time = datetime.datetime.utcnow()
        assert not isinstance(value.chain_state, ModifiableChainstate)
        self.cursor.execute("BEGIN IMMEDIATE")
        self.cursor.execute(
            "SELECT last_modify_time FROM sessions WHERE key = ?", (self.key(key),)
        )
        row = self.cursor.fetchone()
        timestamp = (
            datetime.datetime.fromisoformat(row[0])
            if row is not None
            else datetime.datetime.min
        )
        if timestamp <= value.last_modify_time:
            self.cursor.execute(
                """
            REPLACE INTO sessions (
                key,
                session,
                creation_time,
                last_access_time,
                last_modify_time
            ) VALUES (?, ?, ?, ?, ?)
            """,
                (
                    self.key(key),
                    pickle.dumps(value),
                    value.creation_time.isoformat(),
                    value.last_access_time.isoformat(),
                    value.last_modify_time.isoformat(),
                ),
            )
            self.conn.commit()
        else:
            self.conn.rollback()
            raise RuntimeError(
                f"Session for key {self.key(key)} has been modified since last access and will not be overwritten"
            )
        self.SESSIONS_CACHE[key] = (value, value.last_modify_time)
        self.SESSIONS_CACHE.move_to_end(key)
        while len(self.SESSIONS_CACHE) > 128:
            self.SESSIONS_CACHE.popitem(last=False)

    def __getitem__(self, key: uuid.UUID):
        self.cursor.execute(
            "SELECT last_modify_time FROM sessions WHERE key = ?", (self.key(key),)
        )
        row = self.cursor.fetchone()
        if not row:
            raise KeyError(f"Could not find {self.key(key)} in Session Database")
        timestamp = (
            datetime.datetime.fromisoformat(row[0])
            if row is not None
            else datetime.datetime.min
        )
        if key in self.SESSIONS_CACHE and self.SESSIONS_CACHE[key][0] == timestamp:
            session = self.SESSIONS_CACHE[key][1]
            self.SESSIONS_CACHE.move_to_end(key)
        else:

            self.cursor.execute(
                "SELECT session FROM sessions WHERE key = ?", (self.key(key),)
            )
            row = self.cursor.fetchone()
            session = pickle.loads(row[0])
        session.last_access_time = datetime.datetime.utcnow()
        if not isinstance(session.chain_state, ModifiableChainstate):
            session.chain_state = ModifiableChainstate(
                key, session, session.chain_state, self
            )
        self.SESSIONS_CACHE[key] = (session.last_modify_time, session)
        while len(self.SESSIONS_CACHE) > 128:
            self.SESSIONS_CACHE.popitem(last=False)
        self.cursor.execute("BEGIN IMMEDIATE")
        self.cursor.execute(
            """
            UPDATE sessions
            SET last_access_time = ?
            WHERE key = ?
            """,
            (session.last_access_time.isoformat(), self.key(key)),
        )
        self.conn.commit()
        return session

    def __delitem__(self, key: uuid.UUID):
        self.cursor.execute("DELETE FROM sessions WHERE key = ?", (self.key(key),))
        self.conn.commit()
        self.SESSIONS_CACHE.pop(key, None)

    def __iter__(self):
        self.cursor.execute("SELECT key FROM sessions")
        for row in self.cursor.fetchall():
            yield self.unkey(row[0])

    def __len__(self):
        return len([i for i in self])

    def __contains__(self, key: uuid.UUID) -> bool:
        self.cursor.execute(
            "SELECT 1 FROM sessions WHERE key = ? LIMIT 1", (self.key(key),)
        )
        return self.cursor.fetchone() is not None


class SessionModel(BaseModel):
    creation_time: datetime.datetime
    last_access_time: datetime.datetime


class TransactionInputModel(BaseModel):
    tx_id: str
    output_index: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    # start up logic
    async def cleanup():
        while True:
            sessions = SessionManager()
            next_expiring_session = sessions.cleanup()
            delay = (next_expiring_session - datetime.datetime.utcnow()).total_seconds()
            await asyncio.sleep(delay)

    asyncio.create_task(cleanup())

    yield
    # Shutdown logic


# User Rate Limiter
limiter = Limiter(
    key_func=get_remote_address, default_limits=[SERVER_CONFIG["default_request_limit"]]
)

app = FastAPI(
    title="MockFrost API",
    summary="A clone of the important parts of the BlockFrost API which are used to evaluate transactions. Create your own mocked environment and execute transactions in it.",
    description="""
Start by creating a session.
You will receive a session id, which creates a unique fake blockchain state for you.
Using the session id, you can use `/<session_id>/api/v0` as base url for any Blockfrost using
transaction builder (such as the BlockFrostChainContext in PyCardano, Lucid, MeshJS etc).
The `/session` route provides you with additional tools to manipulate the state of the chain such as creating transaction outputs,
spinning forward the time of the environment or changing the protocol parameters.

Refer to the [Blockfrost documentation](https://docs.blockfrost.io/) for more details about the `api/v0/` subroutes.

You can find more details about this project and the source code on [GitHub](https://github.com/opshin/plutus-bench).

There are two variants of this documentation available:
- [Swagger UI](/docs): A more interactive documentation with a UI.
- [Redoc](/redoc): A more static documentation with a focus on readability.
""",
    lifespan=lifespan,
)
from fastapi.responses import RedirectResponse


@app.exception_handler(RateLimitExceeded)
async def custom_rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={
            "status_code": 422,
            "error": "Too many requests",
            "message": f"Please wait before making more requests. retry_after: {exc.detail}",  # or exc.headers.get("Retry-After")
        },
        headers=exc.headers,  # Important to keep headers for clients to back off
    )


app.state.limiter = limiter
app.state.SERVER_CONFIG = SERVER_CONFIG
app.add_exception_handler(RateLimitExceeded, custom_rate_limit_handler)
app.add_middleware(SlowAPIMiddleware)


@app.get("/", response_class=RedirectResponse, include_in_schema=False)
async def redirect_fastapi():
    return "/docs"


@app.post("/session")
@limiter.limit(SERVER_CONFIG["ip_session_limit"])
@limiter.limit(
    SERVER_CONFIG["global_shared_session_limit"], key_func=lambda: "shared_key"
)
def create_session(
    request: Request,
    seed: int = 0,
    protocol_parameters: dict = dataclasses.asdict(DEFAULT_PROTOCOL_PARAMETERS),
    genesis_parameters: dict = dataclasses.asdict(DEFAULT_GENESIS_PARAMETERS),
) -> uuid.UUID:
    """
    Create a new session.
    Sets all parameters not specified in protocol and genesis to their default values.
    """
    protocol_parameters = frozendict.frozendict(
        protocol_parameters
    ) | dataclasses.asdict(DEFAULT_PROTOCOL_PARAMETERS)
    genesis_parameters = frozendict.frozendict(genesis_parameters) | dataclasses.asdict(
        DEFAULT_GENESIS_PARAMETERS
    )
    session_id = uuid.uuid4()
    now = datetime.datetime.utcnow()
    SESSIONS = SessionManager()
    SESSIONS[session_id] = Session(
        chain_state=MockFrostApi(
            protocol_param=ProtocolParameters(**protocol_parameters),
            genesis_param=GenesisParameters(**genesis_parameters),
            seed=seed,
        ),
        creation_time=now,
        last_access_time=now,
        last_modify_time=now,
    )
    return session_id


@app.get("/session/{session_id}")
def get_session_info(request: Request, session_id: uuid.UUID) -> Optional[SessionModel]:
    """
    Remove a session after usage.
    """
    session = get_session(session_id)
    if not session:
        return None
    return SessionModel(
        creation_time=session.creation_time,
        last_access_time=session.last_access_time,
    )


@app.delete("/session/{session_id}")
def delete_session(request: Request, session_id: uuid.UUID) -> bool:
    """
    Remove a session after usage.
    """
    SESSIONS = SessionManager()
    if session_id in SESSIONS:
        del SESSIONS[session_id]
        return True
    return False


def model_from_transaction_input(tx_in: TransactionInput):
    return TransactionInputModel(
        tx_id=tx_in.transaction_id.payload.hex(), output_index=tx_in.index
    )


def get_session(session_id):
    SESSIONS = SessionManager()
    if session_id not in SESSIONS:
        raise fastapi.HTTPException(status_code=404, detail="Session not found")
    return SESSIONS[session_id]


@app.post("/{session_id}/ledger/txo")
def add_transaction_output(
    request: Request, session_id: uuid.UUID, tx_cbor: Annotated[str, Body(embed=True)]
) -> TransactionInputModel:
    """
    Add a transaction output to the UTxO, without specifying the transaction hash and index (the "input").
    These will be created randomly and the corresponding CBOR is returned.
    """
    tx_in = get_session(session_id).chain_state.add_txout(
        pycardano.TransactionOutput.from_cbor(tx_cbor)
    )
    return model_from_transaction_input(tx_in)


@app.put("/{session_id}/ledger/utxo")
def add_utxo(
    request: Request, session_id: uuid.UUID, tx_cbor: bytes
) -> TransactionInputModel:
    """
    Add a transaction output and input to the UTxO.
    Potentially overwrites existing inputs with the same transaction hash and index.
    Returns the created transaction input.
    """
    utxo = pycardano.UTxO.from_cbor(tx_cbor)
    get_session(session_id).chain_state.add_utxo(utxo)
    return model_from_transaction_input(utxo.input)


@app.delete("/{session_id}/ledger/txo")
def delete_transaction_output(
    request: Request, session_id: uuid.UUID, tx_input: TransactionInputModel
) -> bool:
    """
    Delete a transaction output from the UTxO.
    Returns whether the transaction output was in the UTxO
    """
    try:
        get_session(session_id).chain_state.remove_txi(
            TransactionInput(
                transaction_id=TransactionId(tx_input.tx_id),
                index=tx_input.output_index,
            )
        )
    except:
        return False


@app.put("/{session_id}/ledger/slot")
def set_slot(request: Request, session_id: uuid.UUID, slot: int) -> int:
    """
    Set the current slot of the ledger to a specified value.
    Essentially acts as a "time travel" tool.
    """
    get_session(session_id).chain_state.set_block_slot(slot)
    return slot


@app.put("/{session_id}/pools/pool")
def add_pool(
    request: Request, session_id: uuid.UUID, pool_id: Annotated[str, Body(embed=True)]
) -> str:
    """
    Add a fake staking pool. This may be delegated to mimic rewards.
    """
    get_session(session_id).chain_state.add_mock_pool(pool_id)
    return pool_id


@app.put("/{session_id}/pools/distribute")
def distribute_rewards(request: Request, session_id: uuid.UUID, rewards: int) -> int:
    """
    Distributed rewards to staked accounts. Emulates the behaviour of reward distribution at epoch boundaries.
    """
    get_session(session_id).chain_state.distribute_rewards(rewards)
    return rewards


@app.get("/{session_id}/api/v0/epochs/latest")
def latest_epoch(request: Request, session_id: uuid.UUID) -> dict:
    """
    Return the information about the latest, therefore current, epoch.

    https://docs.blockfrost.io/#tag/Cardano-Epochs/paths/~1epochs~1latest/get
    """
    session = get_session(session_id)
    return session.chain_state.epoch_latest(return_type="json")


@app.get("/{session_id}/api/v0/blocks/latest")
def latest_block(request: Request, session_id: uuid.UUID) -> dict:
    """
    Return the latest block available to the backends, also known as the tip of the blockchain.

    https://docs.blockfrost.io/#tag/Cardano-Blocks/paths/~1blocks~1latest/get
    """
    return get_session(session_id).chain_state.block_latest(return_type="json")


@app.get("/{session_id}/api/v0/genesis")
def genesis(request: Request, session_id: uuid.UUID) -> dict:
    """
    Return the information about blockchain genesis.

    https://docs.blockfrost.io/#tag/Cardano-Ledger/paths/~1genesis/get
    """
    return get_session(session_id).chain_state.genesis(return_type="json")


@app.get("/{session_id}/api/v0/epochs/latest/parameters")
def latest_epoch_protocol_parameters(request: Request, session_id: uuid.UUID) -> dict:
    """
    Return the protocol parameters for the latest epoch.

    https://docs.blockfrost.io/#tag/Cardano-Epochs/paths/~1epochs~1latest~1parameters/get
    """
    return get_session(session_id).chain_state.epoch_latest_parameters(
        return_type="json"
    )


@app.get("/{session_id}/api/v0/scripts/{script_hash}")
def specific_script(request: Request, session_id: uuid.UUID, script_hash: str) -> dict:
    """
    Information about a specific script

    https://docs.blockfrost.io/#tag/Cardano-Scripts/paths/~1scripts~1%7Bscript_hash%7D/get
    """
    return get_session(session_id).chain_state.script(
        script_hash=script_hash, return_type="json"
    )


@app.get("/{session_id}/api/v0/scripts/{script_hash}/cbor")
def script_cbor(request: Request, session_id: uuid.UUID, script_hash: str) -> dict:
    """
    CBOR representation of a `plutus` script

    https://docs.blockfrost.io/#tag/Cardano-Scripts/paths/~1scripts~1%7Bscript_hash%7D~1cbor/get
    """
    return get_session(session_id).chain_state.script_cbor(
        script_hash=script_hash, return_type="json"
    )


@app.get("/{session_id}/api/v0/scripts/{script_hash}/json")
def script_json(request: Request, session_id: uuid.UUID, script_hash: str) -> dict:
    """
    JSON representation of a `timelock` script

    https://docs.blockfrost.io/#tag/Cardano-Scripts/paths/~1scripts~1%7Bscript_hash%7D~1json/get
    """
    return get_session(session_id).chain_state.script_cbor(
        script_hash=script_hash, return_type="json"
    )


@app.get("/{session_id}/api/v0/addresses/{address}/utxos")
def address_utxos(request: Request, session_id: uuid.UUID, address: str) -> list:
    """
    UTXOs of the address.

    https://docs.blockfrost.io/#tag/Cardano-Addresses/paths/~1addresses~1%7Baddress%7D~1utxos/get
    """
    return get_session(session_id).chain_state.address_utxos(
        address=address, return_type="json"
    )


@app.post("/{session_id}/api/v0/tx/submit")
@limiter.limit(SERVER_CONFIG["ip_transaction_limit"])
def submit_a_transaction(
    request: Request,
    session_id: uuid.UUID,
    transaction: Annotated[bytes, Body(media_type="application/cbor")],
) -> str:
    """
    Submit an already serialized transaction to the network.

    https://docs.blockfrost.io/#tag/Cardano-Transactions/paths/~1tx~1submit/post
    """
    try:
        return get_session(session_id).chain_state.transaction_submit_raw(
            transaction, return_type="json"
        )
    except InvalidTransactionError as e:
        return JSONResponse(
            status_code=422,
            content={
                "status_code": 422,
                "message": str(e),
                "error": "Invalid Transaction.",
            },
        )
        # raise HTTPException(status_code = 422, detail = {"message": str(e)})


@app.post("/{session_id}/api/v0/utils/txs/evaluate")
@limiter.limit(SERVER_CONFIG["ip_transaction_limit"])
def submit_a_transaction_for_execution_units_evaluation(
    request: Request,
    session_id: uuid.UUID,
    transaction: Annotated[str, Body(media_type="application/cbor")],
) -> dict:
    """
    Submit an already serialized transaction to evaluate how much execution units it requires

    https://docs.blockfrost.io/#tag/Cardano-Utilities/paths/~1utils~1txs~1evaluate/post
    """
    return get_session(session_id).chain_state.transaction_evaluate_raw(
        bytes.fromhex(transaction), return_type="json"
    )


@app.get("/{session_id}/api/v0/accounts/{stake_address}")
def specific_account_address(
    request: Request, session_id: uuid.UUID, stake_address: str
) -> dict:
    """
    Obtain information about a specific stake account

    https://docs.blockfrost.io/#tag/cardano--accounts/GET/accounts/{stake_address}
    """
    return get_session(session_id).chain_state.accounts(
        stake_address=stake_address, return_type="json"
    )
