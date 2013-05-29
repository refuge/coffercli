%%% -*- erlang -*-
%%%
%%% This file is part of coffercli released under the Apache license 2.
%%% See the NOTICE for more information.
%%%
-module(coffercli).

-export([start/0, stop/0]).

-export([new_connection/1, new_connection/2,
         close_connection/1,
         ping/1,
         containers/1]).

-include("coffercli.hrl").

-compile([{parse_transform, hackney_transform}]).

%% @doc Start the coffer application. Useful when testing using the shell.
start() ->
    coffercli_deps:ensure(),
    application:load(coffercli),
    coffercli_app:ensure_deps_started(),
    application:start(coffercli).

%% @doc Start the coffer application. Useful when testing using the shell.
stop() ->
    application:stop(coffercli).


new_connection(URL) ->
    new_connection(URL, []).

new_connection(URL, Opts) when is_list(URL) ->
    new_connection(list_to_binary(URL), Opts);
new_connection(URL, Opts) ->
    Opts1 = case proplists:get_value(pool, Opts) of
        undefined ->
            PoolOpts = proplists:get_value(pool_opts, Opts, [{pool_size, 10}]),
            PoolName = list_to_atom(binary_to_list(<<"Pool:", URL/binary>>)),
            {ok, Pool} = hackney:start_pool(PoolName, PoolOpts),
            [{pool, Pool} | Opts];
        _ ->
            Opts
    end,
    #coffer_conn{url = URL,
                 options = Opts1,
                 state = active}.

close_connection(#coffer_conn{options=Opts}) ->
    Pool = proplists:get_value(pool, Opts),
    hackney:stop_pool(Pool).


ping(#coffer_conn{state=closed}) ->
    {error, closed};
ping(#coffer_conn{url=URL, options=Opts}) ->
    case hackney:head(URL, [], <<>>, Opts) of
        {ok, 200, _, Client} ->
            hackney:close(Client),
            pong;
        _ ->
            pang
    end.

containers(#coffer_conn{state=closed}) ->
    {error, closed};
containers(#coffer_conn{url=URL, options=Opts}) ->
    URL1 = iolist_to_binary([URL, "/containers"]),
    case coffercli_util:request(get, URL1, [200], Opts) of
         {ok, _, _, JsonBin} ->
            JsonObj = jsx:decode(JsonBin),
            Containers = proplists:get_value(<<"containers">>, JsonObj),
            {ok, Containers};
        Error ->
            Error
    end.

