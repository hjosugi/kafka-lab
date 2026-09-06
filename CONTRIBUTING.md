# Contributing

Start with the [README](README.md), [architecture](docs/architecture.md), and
[validation record](docs/validation.md). The main lab and comparison examples have different semantics.

Before proposing a change:

```bash
bash scripts/lab.sh test
python3 scripts/check.py
```

For adapter, SQL, image, or Compose changes, run the affected integration example in a fresh lab.
Include the command, pinned version, expected result, and actual result. Do not describe a YAML parse
as a successful integration test. Keep fixtures small and expected values independently reviewable.

Document changes to delivery guarantees, retention, event identity, and compatibility. Use original
diagrams and link upstream references. Do not copy third-party course text or images into the project.
Check [THIRD_PARTY.md](THIRD_PARTY.md) when adding software or assets.
