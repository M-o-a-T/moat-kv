"""
This module contains the default values for distkv configuration.
"""

from .util import attrdict

__all__ = ["PORT", "CFG"]

PORT = 27586  # 20000 + 100*ord('K') + ord('V')

# This default configuration will be used to supplement whatever
# configuration you use.
# It is "complete" in the sense that DistKV will never die
# due to a KeyError caused by a missing config value.

CFG = attrdict(
    logging={ # a magic incantation
        "version":1,
        "loggers":{
            "asyncserf":{"level":"INFO"},
        },
        "root":{
            "handlers":["stderr",],
            "level":"INFO",
        },
        "handlers":{
            "logfile":{
                "class":"logging.FileHandler",
                "filename":"test.log",
                "level":"DEBUG",
                "formatter":"std",
            },
            "stderr":{
                "class":"logging.StreamHandler",
                "level":"DEBUG",
                "formatter":"std",
                "stream":"ext://sys.stderr",
            },
        },
        "formatters":{
            "std":{
                "class":"distkv.util.TimeOnlyFormatter",
                "format":'%(asctime)s %(levelname)s:%(name)s:%(message)s',
            },
        },
        "disable_existing_loggers":False,

    },
    connect=attrdict(
        # client: controls talking to the DistKV server
        host="localhost",
        port=PORT,
        ssl=False,
        # ssl=attrdict(cert='/path/to/cert.pem',key='/path/to/cert.key'),
        init_timeout=5,  # time to wait for connection plus greeting
        auth=None,  # no auth used
    ),
    errors=attrdict(
        prefix=('.distkv','error'),
    ),
    codes=attrdict(
        prefix=('.distkv','code','proc'),
    ),
    modules=attrdict(
        prefix=('.distkv','code','module'),
    ),
    runner=attrdict(
        prefix=('.distkv','run'),
        start_delay=1,
        actor=attrdict(
            cycle=5,
            nodes=-1,
            splits=5,
        ),
    ),

    server=attrdict(
        # server-side configuration
        serf=attrdict(
            # how to connect to Serf
            host="localhost",
            port=7373,
        ),
        root=":distkv",  # user event prefix. Should start with a colon.
        paranoia=False,  # typecheck server-to-server updates?

        # which addresses/ports to accept DistKV connections on
        bind=[attrdict()],
        bind_default=attrdict(  # default values for all elements of "bind"
            host="localhost",
            port=PORT,
            ssl=False,
        ),

        state=None,  # path to a file load/save system state
        change=attrdict(length=5),  # chain length: use max nr of network sections +1
        ping=attrdict(cycle=5, gap=2),  # asyncserf.Actor config timing for ping
        core=attrdict(cycle=5, gap=2),  # asyncserf.Actor config timing for core
        # ping time also controls minimum startup time
    ),
    paranoia=False,  # typecheck server>server updates?

    # how does a new server reach existing nodes, to download state?
    domain=None,  # domain in which to look up node names, if not in hostmap
    hostmap = {  # map DistKV server names to connect destinations
        "test1": ("localhost",PORT),
        "test2": ("does-not-exist.invalid",PORT),
    },
)
