import logging
import os

logger = logging.getLogger(__name__)

from moat.util import yload


def load_cfg(cfg):  # pylint: disable=redefined-outer-name
    if os.path.exists(cfg):
        pass
    elif os.path.exists(os.path.join("tests", cfg)):  # pragma: no cover
        cfg = os.path.join("tests", cfg)
    elif os.path.exists(os.path.join(os.pardir, cfg)):  # pragma: no cover
        cfg = os.path.join(os.pardir, cfg)
    else:  # pragma: no cover
        raise RuntimeError(f"Config file {cfg!r} not found")

    with open(cfg, "r", encoding="utf-8") as f:
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
