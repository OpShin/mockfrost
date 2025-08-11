---
layout: home
title: About
nav_order: 1
---

<div align="center">
<img alt="A person with the Cardano logo as face, lifting heavy weights" src="{{ '/assets/plutus-bench.png' | relative_url }}" width="240" />
<h1>Plutus-Bench</h1>
</div>


Test and benchmark Plutus Smart Contracts.
This project enables you to construct entire smart contract transactions against a mock environment, and measure the correctness (i.e. unit test) and performance of your smart contracts.

Plutus Bench creates a mock ledger with an arbitrary UTxO set specified by the user.
It then hosts a mock blockfrost API to interact with the ledger, supporting popular off-chain tooling
like [translucent](https://github.com/antibody-cardano/translucent) and [pycardano](https://pycardano.readthedocs.io/en/latest/).

> Note: Plutus Bench is currently a Work In Progress, and is not yet ready for production use.

### Why Plutus Bench?

- **Compatability**: Plutus Bench is written in Python, and is compatible with popular off-chain tooling relying on BlockFrost APIs. Ogmios mocking is also planned.
- **Holistic Testing**: Plutus Bench allows you to test the entire lifecycle of a smart contract, from minting to consuming, in a single test.
- **Simple**: Plutus Bench is designed to be simple to use, and easy to integrate with your existing infrastructure.