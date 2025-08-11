---
title: Usage
nav_order: 3
permalink: /usage/
---

# Usage


Generally the workflow is as follows:

- Create a new session with the `/session/create` endpoint. You receive a session ID.
- Initialize a BlockFrost client in your off-chain code, and point it to the mock server. The base url is `http://localhost:8000/<session-id>/api/v1`. Project id is not required.

That's it! You can now interact with the mock ledger using the BlockFrost client.

You may further manipulate the ledger using the `/<session-id>/ledger` endpoints.