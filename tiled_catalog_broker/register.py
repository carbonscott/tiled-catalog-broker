#!/usr/bin/env python
"""
Generic HTTP registration CLI (thin wrapper).

See broker.cli.register_main for the full implementation.

Usage:
    python register.py datasets/vdp.yml
    python register.py datasets/vdp.yml datasets/edrixs.yml
    python register.py datasets/edrixs.yml -n 5
"""
from broker.cli import register_main

if __name__ == "__main__":
    register_main()
