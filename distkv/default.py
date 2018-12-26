from .util import attrdict

__all__ = [
        'PORT', 'CFG',
    ]

PORT = 27586  # 2000 + 100*ord('K') + ord('V')

CFG = attrdict(
        server=attrdict(
            host="localhost",
            port=PORT,
            ),
        serf=attrdict(
            host="localhost",
            port=7373,
            ),
    )
