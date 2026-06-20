# Python-play

## Pipeline demo

A small demo script that spawns multiple worker processes which communicate via TCP sockets.

### Setup

Install dependencies with `uv`:

```bash
uv sync
```

### Run the demo

```bash
uv run python pipeline.py --num 3 --topology ring --duration 10
```

Or directly:

```bash
python3 pipeline.py --num 3 --topology ring --duration 10
```

### Options

See [pipeline.py](pipeline.py) for available flags:
- `--num`: number of worker processes (default: 3)
- `--topology`: ring, star, or all (default: ring)
- `--duration`: seconds to run (default: 10)
- `--msgs`: messages per peer per send loop (default: 1)
- `--interval`: seconds between messages (default: 0.2)