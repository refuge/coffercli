%%% -*- erlang -*-
%%%
%%% This file is part of coffercli released under the Apache license 2.
%%% See the NOTICE for more information.

-record(coffer_conn, {url,
                      options,
                      state}).

-record(remote_storage, {conn,
                         conn_options,
                         url,
                         name}).


-ifdef(sha_workaround).
-define(SHA(Data), crypto:sha(Data)).
-define(SHA_INIT(), crypto:sha_init()).
-define(SHA_UPDATE(Ctx, Data), crypto:sha_update(Ctx, Data)).
-define(SHA_FINAL(Ctx), crypto:sha_final(Ctx)).
-else.
-define(SHA(Data), crypto:hash(sha, Data)).
-define(SHA_INIT(), crypto:hash_init(sha)).
-define(SHA_UPDATE(Ctx, Data), crypto:hash_update(Ctx, Data)).
-define(SHA_FINAL(Ctx), crypto:hash_final(Ctx)).
-endif.
