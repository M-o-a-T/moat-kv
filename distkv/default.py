"""
This module contains the default values for distkv configuration.
"""

from .util import attrdict, combine_dict, NotGiven, P
from .ext import list_ext, load_ext

__all__ = ["PORT", "CFG"]

PORT = 27586  # 20000 + 100*ord('K') + ord('V')

# This default configuration will be used to supplement whatever
# configuration you use.
# It is "complete" in the sense that DistKV will never die
# due to a KeyError caused by a missing config value.

CFG = attrdict(
    logging={  # a magic incantation
        "version": 1,
        "loggers": {"asyncserf": {"level": "INFO"}, "xknx.raw_socket": {"level": "INFO"}},
        "root": {"handlers": ["stderr"], "level": "INFO"},
        "handlers": {
            #           "logfile": {
            #               "class": "logging.FileHandler",
            #               "filename": "test.log",
            #               "level": "DEBUG",
            #               "formatter": "std",
            #           },
            "stderr": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "std",
                "stream": "ext://sys.stderr",
            }
        },
        "formatters": {
            "std": {
                "class": "distkv.util.TimeOnlyFormatter",
                "format": "%(asctime)s %(levelname)s:%(name)s:%(message)s",
            }
        },
        "disable_existing_loggers": False,
    },
    connect=attrdict(
        # client: controls how to talk to the DistKV server
        host="localhost",
        port=PORT,
        ssl=False,
        # ssl=attrdict(cert='/path/to/cert.pem',key='/path/to/cert.key'),
        init_timeout=5,  # time to wait for connection plus greeting
        auth=None,  # no auth used by default
        name=None,  # defaults to the server's name
    ),
    config=attrdict(prefix=P(":.distkv.config")),
    errors=attrdict(prefix=P(":.distkv.error")),
    codes=attrdict(prefix=P(":.distkv.code.proc")),
    modules=attrdict(prefix=P(":.distkv.code.module")),
    runner=attrdict(  # for distkv.runner.RunnerRoot
        prefix=P(":.distkv.run"),  # storage for runnable commands
        state=P(":.distkv.state"),  # storage for runner states
        name="run",  # Serf event name, suffixed by subpath
        start_delay=1,  # time to wait between job starts. Not optional.
        ping=-15,  # set an I-am-running message every those-many seconds
        # positive: set in distkv, negative: broadcast to :distkv:run tag
        actor=attrdict(cycle=10, nodes=-1, splits=5),  # Actor config  # required for Runner
        sub=attrdict(group="any", single="at", all="all"),  # tags for various runner modes
    ),
    server=attrdict(
        # server-side configuration
        backend=NotGiven,  # must be specified
        serf=attrdict(
            # how to connect to Serf
            host="localhost",
            port=7373,
        ),
        root=(":distkv",),  # event message name prefix. Should start with a colon.
        paranoia=False,  # typecheck server-to-server updates?
        # which addresses/ports to accept DistKV connections on
        bind=[attrdict()],
        bind_default=attrdict(  # default values for all elements of "bind"
            host="localhost", port=PORT, ssl=False
        ),
        change=attrdict(length=5),  # chain length: use max nr of network sections +1
        ping=attrdict(cycle=10, gap=2),  # asyncserf.Actor config timing for ping
        delete=attrdict(cycle=10, gap=2),  # asyncserf.Actor config timing for deletion
        # ping time also controls minimum startup time
    ),
    paranoia=False,  # typecheck server>server updates?
    # how does a new server reach existing nodes, to download state?
    domain=None,  # domain in which to look up node names, if not in hostmap
    hostmap={  # map DistKV server names to connect destinations
        "test1": ("localhost", PORT),
        "test2": ("does-not-exist.invalid", PORT),
    },
)

for n, _ in list_ext("config"):  # pragma: no cover
    CFG[n] = combine_dict(load_ext(n, "config", "CFG"), CFG.get(n, {}), cls=attrdict)
