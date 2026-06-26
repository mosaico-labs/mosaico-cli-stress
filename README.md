# mosaico-stress

Stress test extension for the Mosaico CLI.

## Installation

```bash
pip install mosaico-stress
```

Once installed, the `mosaico-stress` binary is available in your `$PATH` and automatically discovered by the Mosaico CLI as an extension:

```bash
mosaico stress upload --client=100 --size=10GB --time=5m
mosaico stress download --client=50 --size=5GB --time=2m
```

## Commands

- `upload` — Concurrent upload stress test with random data generation
- `download` — Concurrent download stress test using existing platform data
