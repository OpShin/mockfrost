<div align="center">
<img alt="A person with the Cardano logo as face, lifting heavy weights" src="plutus-bench.png" width="240" />
<h1>MockFrost / Plutus-Bench</h1>
</div>

Test and benchmark Plutus Smart Contracts.
This project enables you to construct entire smart contract transactions against a mock environment, and measure the correctness (i.e. unit test) and performance of your smart contracts.

Plutus Bench creates a mock ledger with an arbitrary UTxO set specified by the user.
It then hosts a mock blockfrost API to interact with the ledger, supporting popular off-chain tooling
like [translucent](https://github.com/antibody-cardano/translucent) and [pycardano](https://pycardano.readthedocs.io/en/latest/).

> Note: Plutus Bench is currently in Beta. Expect rough edges!

### Why Plutus Bench?

- **Compatability**: Plutus Bench is written in Python, and is compatible with popular off-chain tooling relying on BlockFrost APIs. Ogmios mocking is also planned.
- **Holistic Testing**: Plutus Bench allows you to test the entire lifecycle of a smart contract, from minting to consuming, in a single test.
- **Simple**: Plutus Bench is designed to be simple to use, and easy to integrate with your existing infrastructure.

### Installation and running

```bash
# install the plutus-bench package
git clone https://github.com/OpShin/plutus-bench.git
cd plutus-bench
# install the package
pip install .
bash ensure_pure_cbor2.sh
# run the mock server
uvicorn plutus_bench.mockfrost.server:app 
```

After running these commands, a mock blockfrost server will be running on `http://localhost:8000`.
Head to `http://localhost:8000/docs` to see the API documentation.

### Docker

```bash
docker compose up --build
```

This starts MockFrost on `http://localhost:8000` and persists session data in a Docker volume.
To stop it:

```bash
docker compose down
```

### Usage

Generally the workflow is as follows:

- Create a new session with the `/session/create` endpoint. You receive a session ID.
- Initialize a BlockFrost client in your off-chain code, and point it to the mock server. The base url is `http://localhost:8000/<session-id>/api/v1`. Project id is not required.

That's it! You can now interact with the mock ledger using the BlockFrost client.

You may further manipulate the ledger using the `/<session-id>/ledger` endpoints.

## Tutorials and Walkthroughs

- Concrete example and introduction (Reddit): https://www.reddit.com/r/CardanoDevelopers/comments/1j2irs1/introducing_mockfrost_plutusbench_for_endtoend/
- Walkthrough: How to adapt your off-chain scripts to MockFrost (YouTube): https://youtu.be/B2wZd1xDSMU

### Similar Projects

- [lucid transaction emulator](https://lucid.spacebudz.io/) - Allows emulation of transactions on the Cardano blockchain. Only supports JavaScript and TypeScript.
- [helios transaction emulator](https://github.com/hyperion-bt/helios) - Allows emulation of transactions on the Cardano blockchain. Only supports JavaScript and TypeScript.
- [plutus simple model](https://github.com/mlabs-haskell/plutus-simple-model) - Allows evaluating Plutus smart contracts. Only supports Haskell.

### What about the language comparison?

The language comparison has been moved to [contest](./contest).
It is planned to expand to cover different implementations of the same contract in different languages and the same language, tailored for performance optimizations.
