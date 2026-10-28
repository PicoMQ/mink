import json
import pathlib

import datasketches

FIXTURES = pathlib.Path(__file__).resolve().parents[3] / "conformance" / "fixtures" / "statistics"
LG_K = 12


def sketch(strings, integers):
    built = datasketches.update_theta_sketch(LG_K)

    for text in strings:
        built.update(text)
    for integer in integers:
        built.update(integer)

    return built.compact(True).serialize().hex()


def case(name, strings=(), integers=()):
    updates = [text.encode("ascii").hex() for text in strings]
    updates += [integer.to_bytes(8, "little", signed=True).hex() for integer in integers]

    return {"name": name, "updates": updates, "sketch": sketch(strings, integers)}


CASES = [
    case("empty"),
    case("few_strings", strings=["alpha", "beta", "gamma", "beta"]),
    case("empty_string_is_skipped", strings=["", "x"]),
    case("control_bytes", strings=["\x00\x07", "\x7f", "a\x00b"]),
    case("many_strings", strings=[f"key-{index}" for index in range(20_000)]),
    case("longs", integers=[-(2**63), -1, 0, 1, 2**63 - 1]),
    case("many_longs", integers=[index * 7919 - 50_000_000 for index in range(30_000)]),
]


def main():
    FIXTURES.mkdir(parents=True, exist_ok=True)
    path = FIXTURES / "sketches.json"
    path.write_text(json.dumps({"lg_k": LG_K, "cases": CASES}, indent=2) + "\n")

    print(f"wrote {path}")


main()
