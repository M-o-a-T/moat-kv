import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

from moat.util import yload


def load_cfg(cfg):  # pylint: disable=redefined-outer-name
    cfg = Path(cfg).absolute()
    if cfg.exists():
        pass
    elif (ct := cfg.parent / "tests" / cfg.name).exists():  # pragma: no cover
        cfg = ct
    elif (ct := cfg.parent.parent / cfg.name).exists():  # pragma: no cover
        cfg = ct
    else:  # pragma: no cover
        raise RuntimeError(f"Config file {cfg!r} not found")

    with cfg.open("r", encoding="utf-8") as f:
        cfg = yload(f)

    from logging.config import dictConfig

    cfg["disable_existing_loggers"] = False
    try:
        dictConfig(cfg)
    except ValueError:
        pass
    logging.captureWarnings(True)
    logger.debug("Test %s", "starting up")
    return cfg


cfg = load_cfg(os.environ.get("LOG_CFG", "logging.cfg"))


import trio._core._run as tcr

if "PYTHONHASHSEED" in os.environ:
    tcr._ALLOW_DETERMINISTIC_SCHEDULING = True
    tcr._r.seed(os.environ["PYTHONHASHSEED"])
