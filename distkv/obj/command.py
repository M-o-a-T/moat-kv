# command line interface helpers for objects

import sys
import asyncclick as click

from distkv.util import NotGiven

import logging

logger = logging.getLogger(__name__)

__all__ = ["std_command"]


class _InvSub:
    """
    This class is a helper for "std_command".
    """

    def __init__(
        self,
        name,
        id_name="name",
        id_typ=str,
        aux=(),
        name_cb=None,
        id_cb=None,
        postproc=None,
        apply=None,
        short_help=None,
        sub_base=None,
        sub_name=None,
        long_name=None,
    ):
        self.name = name
        self.id_name = id_name
        self.id_typ = id_typ
        self.id_cb = id_cb or (lambda _c, _k, x: x)
        self.apply = apply or (lambda _c, _x: None)
        self.name_cb = name_cb or (lambda _c, _k, x: x)
        self.aux = aux
        self.short_help = short_help
        self.postproc = postproc or (lambda _c, x: None)
        self.long_name = long_name or name
        self.sub_base = sub_base
        if sub_name is NotGiven:
            self.sub_name = None
        else:
            self.sub_name = sub_name or name

    def id_arg(self, proc):
        if self.id_name is None:
            return proc
        return click.argument(self.id_name, type=self.id_typ, callback=self.id_cb, nargs=1)(proc)

    def apply_aux(self, proc):
        for t in self.aux:
            proc = t(proc)
        return proc


def std_command(cli, *a, **kw):
    """
    This procedure builds the interface for an inventory-ish thing.

    Usage::

        @click.group(short_help="Manage computer inventory.")
        @click.pass_obj
        async def cli(obj):
            "Inventorize your computers, networks, and their connections."
            obj.inv = await InventoryRoot.as_handler(obj.client)

        std_command(
            cli,  # from above
            "vlan",  # subcommand name
            "id",  # item identifier
            int,  # item identifier type
            aux=(  # additional attributes
                click.option("-d", "--desc", type=str, default=None, help="Description"),
                click.option("-w", "--wlan", type=str, default=None, help="WLAN SSID"),
            ),
            short_help="Manage VLANs",
        )
    """
    tinv = _InvSub(*a, **kw)
    tname = tinv.name
    tnname = "n_" + tname

    def this(obj):
        # Delayed resolving of the actual thing subhierarchy
        if tinv.sub_base:
            data = getattr(obj, tinv.sub_base)
        else:
            data = obj.data
        if tinv.sub_name:
            return getattr(data, tinv.sub_name)
        return data

    @cli.group(
        name=tname,
        invoke_without_command=True,
        short_help=tinv.short_help,
        help="""\
            Manager for {tlname}s.

            \b
            Use '… {tname} -' to list all entries.
            Use '… {tname} NAME' to show details of a single entry.
            """.format(
            tname=tname, tlname=tinv.long_name
        ),
    )
    @click.argument("name", type=str, nargs=1)
    @click.pass_context
    async def typ(ctx, name):
        obj = ctx.obj
        if name == "-":
            if ctx.invoked_subcommand is not None:
                raise click.BadParameter("The name '-' triggers a list and precludes subcommands.")
            cnt = 0
            for n in this(obj):
                cnt += 1
                print(n, file=obj.stdout)
            if not cnt and ctx.obj.debug:
                print("no entries", file=sys.stderr)
        elif ctx.invoked_subcommand is None:
            # Show data from a single entry
            n = this(obj).by_name(name)
            if n is None:
                raise KeyError(name)
            cnt = 0
            for k in n.ATTRS + getattr(n, "AUX_ATTRS", ()):
                v = getattr(n, k, None)
                if v is not None:
                    cnt += 1
                    if isinstance(v, dict):
                        v = v.items()
                    if isinstance(v, type({}.items())):  # pylint: disable=W1116
                        for kk, vv in sorted(v):
                            if isinstance(vv, (tuple, list)):
                                if vv:
                                    vv = " ".join(str(x) for x in vv)
                                else:
                                    vv = "-"
                            elif isinstance(vv, dict):
                                vv = " ".join("%s=%s" % (x, y) for x, y in sorted(vv.items()))
                            print("%s %s %s" % (k, kk, vv), file=obj.stdout)
                    else:
                        print("%s %s" % (k, v), file=obj.stdout)
            if not cnt and ctx.obj.debug:
                print("exists, no data", file=sys.stderr)
        else:
            obj[tnname] = name
            try:
                obj[tname] = this(obj).by_name(name)
            except KeyError:
                obj[tname] = None

    def alloc(obj, name):
        # Allocate a new thing
        if isinstance(name, (tuple, list)):
            n = this(obj).follow(name, create=True)
        else:
            n = this(obj).allocate(name)
        return n

    @typ.command(short_help="Add a " + tinv.long_name)
    @tinv.id_arg
    @tinv.apply_aux
    @click.pass_obj
    async def add(obj, **kw):
        name = obj[tnname]
        if obj[tname] is not None:
            raise RuntimeError(f"{name} already exists")
        if tinv.id_name:
            kw["name"] = name
            n = alloc(obj, kw.pop(tinv.id_name))
        else:
            n = alloc(obj, name)

        await _v_mod(n, **kw)

    add.__doc__ = f"""
        Add a %s
        """ % tinv.long_name

    @typ.command("set", short_help="Modify a " + tinv.long_name)
    @tinv.apply_aux
    @click.pass_obj
    async def set_(obj, **kw):
        name = obj[tnname]
        n = obj[tname]
        if n is None:
            raise KeyError(tname)

        await _v_mod(n, **kw)

    set_.__doc__ = (
        """
        Modify a %s
        """
        % tinv.long_name
    )

    @typ.command(short_help="Delete a " + tinv.long_name)
    @click.pass_obj
    async def delete(obj, **kw):  # pylint: disable=unused-argument,unused-variable
        name = obj[tnname]
        n = this(obj).by_name(name)
        if n is not None:
            await n.delete(recursive=True)

    delete.__doc__ = (
        """
        Delete a %s
        """
        % tinv.long_name
    )

    async def _v_mod(obj, **kw):
        tinv.apply(obj, kw)
        for k, v in kw.items():
            if v:
                if v == "-":
                    v = None
                try:
                    setattr(obj, k, v)
                except AttributeError:
                    if k != "name":
                        raise AttributeError(k, v) from None
        tinv.postproc(obj, kw)
        await obj.save()

    # Finally, return the CLI so the user can attach more stuff
    return typ
