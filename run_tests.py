"""
Standalone Test Runner for PingGuard Chapter 1.
"""

import sys
import unittest
from pathlib import Path

# Ensure pingguard root is in sys.path
PINGGUARD_ROOT = Path(__file__).resolve().parent
if str(PINGGUARD_ROOT) not in sys.path:
    sys.path.insert(0, str(PINGGUARD_ROOT))

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover(str(PINGGUARD_ROOT / "tests"))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
