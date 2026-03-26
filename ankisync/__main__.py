"""Allow running ankisync as a module: python -m ankisync"""

import sys

from ankisync.cli import main

sys.exit(main())
