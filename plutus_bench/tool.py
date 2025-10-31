import json
import pathlib

import cbor2
import pycardano

from enum import Enum


class ScriptType(Enum):
    NativeScript = "NativeScript"
    PlutusV1 = "PlutusV1"
    PlutusV2 = "PlutusV2"
    PlutusV3 = "PlutusV3"


def load_contract(
    path: pathlib.Path | str, plutus_version: ScriptType
) -> pycardano.ScriptType:
    script = None
    script_cbor = path / "script.cbor" if path.is_dir() else path
    if script_cbor.exists():
        script = script_cbor.read_bytes()
        try:
            script_text = script.decode("utf-8")
            script = bytes.fromhex(script_text)
        except (UnicodeDecodeError, ValueError) as e:
            pass
        try:
            script = cbor2.loads(script)
        except cbor2.CBORDecodeError as e:
            pass
    plutus_script = path / "script.plutus" if path.is_dir() else path
    if plutus_script.exists():
        try:
            with open(plutus_script, "r") as f:
                script_dict = json.load(f)
            script = cbor2.loads(bytes.fromhex(script_dict["cborHex"]))
            # .plutus is double-cbor wrapped
            script = cbor2.loads(script)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
            pass
    if script is None:
        raise ValueError(f"Could not load script from {path}")

    script = cbor2.dumps(script)
    if plutus_version == ScriptType.PlutusV1:
        return pycardano.PlutusV1Script(script)
    elif plutus_version == ScriptType.PlutusV2:
        return pycardano.PlutusV2Script(script)
    elif plutus_version == ScriptType.PlutusV3:
        return pycardano.PlutusV3Script(script)
    else:
        raise ValueError(f"Unsupported script type {plutus_version}")


def address_from_script(
    script: pycardano.ScriptType, network: pycardano.Network
) -> pycardano.Address:
    script_hash = pycardano.script_hash(script)
    return pycardano.Address(payment_part=script_hash, network=network)
