import sys

import pytest

sys.dont_write_bytecode = True

retcode = pytest.main(["--cov=src", "./tests", "-p", "no:cacheprovider"])

# Fail the cell execution if we have any test failures.
assert retcode == 0, 'The pytest invocation failed. See the log above for details.'
