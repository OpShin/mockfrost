---
layout: page
title: Usage
permalink: /usage/
---

<div align="center">
<h1>Usage</h1>
</div>
Generally the workflow is as follows:

- Create a new session with the `/session/create` endpoint. You receive a session ID.
- Initialize a BlockFrost client in your off-chain code, and point it to the mock server. The base url is `http://localhost:8000/<session-id>/api/v1`. Project id is not required.

That's it! You can now interact with the mock ledger using the BlockFrost client.

You may further manipulate the ledger using the `/<session-id>/ledger` endpoints.



<div align="left">
<h2>CMD Mockfrost server</h2>
</div>

A local mockfrost server can be started with the following command
```bash
uvicorn plutus_bench.mockfrost.server:app 
```
After running these commands, a mock blockfrost server will be running on `http://localhost:8000`.
Head to `http://localhost:8000/docs` to see the API documentation.

<div align="left">
<h2>Python Mockfrost server</h2>
</div>
A local mockfrost server can be started from within python:
```python
import uvicorn
from plutus_bench.mockfrost.server import app

uvicorn.run(app, port=8000)
```

However, in the context of Unit testing the following is recommended Usage.


```python
from multiprocessing import Process
from time import sleep
import pytest
import uvicorn
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


def test_code(server):
    #test code here
    ...
```

<div align="left">
<h2>Using the Mockfrost server with pycardano</h2>
</div>

The mockfrost server can now be accessed via:
```python
from plutus_bench.mockfrost.client import MockFrostClient, MockFrostUser

def test_code(server):
    client = MockFrostClient(base_url="http://127.0.0.1:8000")
    session = client.create_session() 
    context = session.chain_context() 
```
Session is a `plutus_bench.mockfrost.MockFrostSession` instance that provides a python API to manipulate the server:
```python
    session.add_txout(
        pycardano.TransactionOutput(address=..., amount=...,),
    )

```


The `context` created above is a `pycardano.BlockFrostChainContext` instance pointing towards your mockfrost server:
```python
    # get list of all utxos
    utxos = context.utxos(address=...)
    # create and submit pycardano transactions
    txbuilder = pycardano.TransactionBuilder(context=context)
    ... 
    # build transaction
    ... 
    tx = txbuilder.build_and_sign(...)
    context.submit(tx)
```




The `plutus_bench.mockfrost.MockFrostUser` class can be used to create mock users on the server:

```python
    # Create and fund a user
    user = MockFrostUser(session)
    fund(100_000_000)

    # Return list of utxos for user
    utxos = user.utxos()

    # User class holds all required keys and address.
    s, v, a = user.signing_key, user.verification_key, user.address
```



