import pytest

import logging
logging.basicConfig(level=logging.DEBUG)

pytestmark = pytest.mark.usefixtures("force_trio")

