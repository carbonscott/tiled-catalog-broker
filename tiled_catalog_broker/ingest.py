#!/usr/bin/env python
"""
Generic ingest CLI (thin wrapper).

See broker.cli.ingest_main for the full implementation.

Usage:
    python ingest.py datasets/vdp.yml
    python ingest.py datasets/vdp.yml datasets/edrixs.yml
"""
from broker.cli import ingest_main

if __name__ == "__main__":
    ingest_main()
