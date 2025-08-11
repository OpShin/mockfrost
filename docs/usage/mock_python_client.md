---
title: Python Client and API 
parent: Usage
nav_order: 2
---

# Python Mockfrost Client 
{: .no_toc}

## Table of contents
{: .no_toc .text-delta}

1. TOC
{:toc}

## Connect to the server {#client}
Given a [Mockfrost Server]({% link usage/mock_server.md %}) running on the default address `http://127.0.0.1:8000`, a Mockfrost client can be configured to connect to it as follows:
```python
from plutus_bench.mockfrost.client import MockFrostClient, MockFrostUser

def test_code(server):
    # Create client and point it towards local mockfrost address
    client = MockFrostClient(base_url="http://127.0.0.1:8000")

    # The client can be used to create a plutus_bench.mockfrost.MockFrostSession
    # This can be used to directly manipulate your mock chain
    session = client.create_session() 

    # We can create a pycardano.BlockFrostChainContext from the session.
    # You should be able to interact with the local mockfrost session though the chain context 
    # the same way you interact with testnet or mainnet
    context = session.chain_context() 
```

## MockFrostSession
The `session` object [created above](#client) with a `client.create_session()` call is a `plutus_bench.mockfrost.MockFrostSession` instance.
This creates a unique blockchain state on the mockfrost server accessed via the session ID with the base url `http://localhost:8000/<session-id>/api/v0`.
The session ID is held as the attribute `session.session_id`.

The MockFrostSession instance provides a python API to manipulate the ledger.
UTxOs can be added to the mock ledger in two ways:
```python
    session.add_utxo(
        pycardano.UTxO(
            input = pycardano.TransactionInput(transaction_id=..., index=...),
            output = pycardano.TransactionOutput(address=..., amount=...),
        )
    )
    #Alternatively if the input does not matter:
    session.add_txout(
        pycardano.TransactionOutput(address=..., amount=...,),
    )
```

UTxO's can be removed from the ledger via the transaction input:
```python
    session.del_txout(
        pycardano.TransactionInput(transaction_id..., index=...),
    )
```

The slot number of the mock ledger may be directly set using `set_slot`:
```python
    session.set_slot(71_071_542)
```

Additionally, for testing staking it is possible to register a mock pool to delegate to and manually distribute rewards:
```python
    session.add_mock_pool(pycardano.pool_params.PoolId(...))
    session.distribute_rewards(100_000_000) # each correctly delegated account receives this reward
```






## Pycardano ChainContext

The `context` object created [above](#client) is a `pycardano.BlockFrostChainContext` instance pointing towards a mockfrost session on your server.
This should be used to interact with the local network as you would with testnet or mainnet: 
```python
    # get parameters and properties
    print(f'Protocol: {context.protocol_param}')
    print(f'Genesis: {context.genesis_param}')
    print(f'Epoch: {context.epoch}')
    print(f'Slot: {context.last_block_slot}')
    print(f'Network: {context.network}')

    # Retrieve all utxos beloning to an address
    utxos = context.utxos(address=...)

    # create and submit pycardano transactions
    txbuilder = pycardano.TransactionBuilder(context=context)
    ... 
    ... 
    tx = txbuilder.build_and_sign(...)
    context.submit(tx)
```

## Mock Users

The `plutus_bench.mockfrost.MockFrostUser` class can be used to create mock users on the server:

```python
    from plutus_bench.mockfrost import MockFrostUser
    # Create and fund a user
    user = MockFrostUser(session)
    fund(100_000_000)

    # Return list of utxos for user
    utxos = user.utxos()

    # User class holds all required keys and address.
    s, v, a = user.signing_key, user.verification_key, user.address
```

## Mock Pool
For staking you need a mock pool with which to delegate to. This can be most easily achieved with the `plutus_bench.mockfrost.MockFrostPool`:
```python
    from plutus_bench.mockfrost import MockFrostPool
    # Create Pool
    stake_pool = MockFrostPool(session)

    # distribute rewards to delegated accounts
    session.distribute_rewards(100_000_000)


