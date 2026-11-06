#!/usr/bin/env python3
"""Prepare explicit installation/configuration arguments for a private runtime.

The prepared config.ini must already refer to the product's framework, launcher
dependencies and chosen bundles through absolute file locations. This helper
does not build, install, start, or qualify the application.
"""
import argparse
import json
from pathlib import Path


def launcher_arguments(product, configuration, installation):
    product, configuration, installation = [Path(path).resolve() for path in
                                             [product, configuration, installation]]
    if not (product/'plugins').is_dir():
        raise ValueError('product must contain the built plugins directory')
    if not (configuration/'config.ini').is_file():
        raise ValueError('private configuration must already contain config.ini')
    if configuration.is_relative_to(product) or installation.is_relative_to(product):
        raise ValueError('private configuration and installation must be outside the shared product')
    if installation.is_relative_to(configuration) or configuration.is_relative_to(installation):
        raise ValueError('private configuration and installation directories must not overlap')
    if installation.exists() and (not installation.is_dir() or any(installation.iterdir())):
        raise ValueError('use an empty private installation directory')
    installation.mkdir(parents=True, exist_ok=True)
    return ['-install', str(installation), '-configuration', str(configuration)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--product', type=Path, required=True)
    parser.add_argument('--configuration', type=Path, required=True)
    parser.add_argument('--installation', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(launcher_arguments(args.product, args.configuration, args.installation)))


if __name__ == '__main__':
    main()
