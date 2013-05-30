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
