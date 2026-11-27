#!/usr/bin/env python3
"""
TeleRM site node executable.

    python3 site.py --site-id edge-A --config config.json

The implementation lives in common/site_node.py.  (This file cannot hold the
class itself: ``site`` is also a standard-library module, so ``from site
import Site`` would never reach it.)
"""
from common.site_node import Site, Instance, main  # noqa: F401  (Site/Instance re-exported)

if __name__ == "__main__":
    main()
