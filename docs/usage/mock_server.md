---
title: Mockfrost Server
parent: Usage
nav_order: 1
---
# MockFrost Server

{: .no_toc}
## Table of contents
{: .no_toc .text-delta}

1. TOC
{:toc}

## CMD



A local mockfrost server can be started with the following command
```bash
uvicorn plutus_bench.mockfrost.server:app 
```
After running these commands, a mock blockfrost server will be running on `http://localhost:8000`.
Head to `http://localhost:8000/docs` to see the API documentation.




## Python

A local mockfrost server can be started from within python:
```python
import uvicorn
from plutus_bench.mockfrost.server import app

uvicorn.run(app, port=8000)
```

For integration into unit testing we would recommend the following style:


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


def test_contracts(server):
    # test your contracts here
    ...
```

