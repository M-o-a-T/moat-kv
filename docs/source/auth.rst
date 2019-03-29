========================
DistKV and authorization
========================

Current state: there isn't any.

Intended state: client-server, disk-storage, and server-server
communications are protected with strong cryptography and passwords.

Note that inter-server communication uses Serf, which already has strong crypto
layer. Thus DistKV will not further encrypt its transport.

However, DistKV is agnostic about its payloads. Thus, both keys and values
may pass through an encryption layer.

Configuration storage
---------------------

The DistKV core does not restrict which data types may be stored anywhere.
However, storing configuration and authorization data directly in DistKV
makes sense.

DistKV uses a special namespace below its root to store global
configuration and authorization data, below a ``None`` key.
Ordinary requests may not contain these keys.

Initial setup
-------------

Create a record at ``(None,"auth","user","root")`` containing ``{cls='_anon'}``.

After doing this, the initial message from the server contains an
``auth=['_anon']`` entry; any client you use must now log on.

Logging in
----------

API
:::

The client creates an authorizer and runs it on the connection. The data
required for authorization are described by the client's schema. These data
are not directly transmitted to the server because of crypto requirements.

   from distkv.auth import loader
   async def auth_client(client:distkv.client.Client, types):
	  typ = await select_auth_method(types)
	  cls = loader(typ)
	  cls = cls.import("user")
	  name = await get_username()
	  data = await get_data_for_schema(cls.schema)
	  user = cls.build(data)
	  await user.auth(name, client)

Wire
::::

On the wire, this translates to a possibly-streamed "auth" command:

   { seq=1, cmd="auth", name="root", cls="_anon", data=None }

which the server will reply to with an error if login is not possible.

If the auth method requires a more complicated exchange of messages, they must
be streamed:

   C: { seq=1, cmd="auth", state="start", name="root", cls="_userpass"}
   S: { seq=1, state="start", nonce="random_16_byte_string" }
   C: { seq=1, data="nonce_encrypted_with_hashed_password" }
   S: { seq=1, state="end" }
   C: { seq=1, state="end" }

The requirement for streaming is determined by the authentication method in
question.

Auth methods
============

_anon
-----

The anonymous user. This method is not in fact anonymous, as you still need a
user name, but there are no other parameters: the schema is empty.

This method does not support any kind of access control.

wire
::::

   C: { seq=1, cmd="auth", name="foo", cls="_anon" }
   S: { seq=1, value=None }

