import argparse
from pathlib import Path

import yaml

parser = argparse.ArgumentParser()
parser.add_argument("family", choices=("ipv4", "ipv6", "dual"))
parser.add_argument("source", type=Path)
parser.add_argument("destination", type=Path)
args = parser.parse_args()

family = "dual-stack" if args.family == "dual" else args.family
config = yaml.safe_load(args.source.read_text())
for test in config["tft"]:
    for connection in test["connections"]:
        connection.setdefault("ip_family", family)
args.destination.write_text(yaml.safe_dump(config, sort_keys=False))
