#!/usr/bin/env python
"""
Generic manifest generator CLI (thin wrapper).

See broker.cli.generate_main for the full implementation.

Usage:
    python generate.py datasets/vdp.yml -n 10
    python generate.py datasets/vdp.yml datasets/edrixs.yml -n 10
"""
from broker.cli import generate_main

if __name__ == "__main__":
    # In tiled_poc/, generators live in extra/ (not generators/)
    generate_main(default_generators_dir="extra")
