#!/usr/bin/env bash

# This script is used to build the contracts in the contracts directory.

# Get the path of own directory
DIR=$(dirname "${BASH_SOURCE[0]}")
cd $DIR

uv run opshin build contracts/gift.py
uv run opshin build contracts/signed_mint.py --parameters 1
uv run opshin build contracts/unrealistic_staking.py --parameters 1