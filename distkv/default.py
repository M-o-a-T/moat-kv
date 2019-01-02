from .util import attrdict

__all__ = [
        'PORT', 'CFG',
    ]

PORT = 27586  # 2000 + 100*ord('K') + ord('V')

CFG = attrdict(
        server=attrdict(
            host="localhost",
            port=PORT,
            domain=None, # domain in which to look up nodes
            ),
        serf=attrdict(
            host="localhost",
            port=7373,
            ),
        state=None, # path to load/save system state

        root="distkv", # serf user event prefix
        change=attrdict(
            length=5,
            ),
        ping=attrdict(
            length=4,
            clock=5,
            ),
    )
