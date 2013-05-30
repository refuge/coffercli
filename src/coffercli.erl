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
         storages/1,
         storage/2,
         new_blob/2, store/2]).

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

storages(#coffer_conn{state=closed}) ->
    {error, closed};
storages(#coffer_conn{url=URL, options=Opts}) ->
    URL1 = iolist_to_binary([URL, "/containers"]),
    case coffercli_util:request(get, URL1, [200], Opts) of
         {ok, _, _, JsonBin} ->
            JsonObj = jsx:decode(JsonBin),
            Containers = proplists:get_value(<<"containers">>, JsonObj),
            {ok, Containers};
        Error ->
            Error
    end.

storage(#coffer_conn{url=URL, options=Opts}=Conn, Name) ->
    StorageURL = iolist_to_binary([URL, "/", Name]),
    #remote_storage{conn = Conn,
                    conn_options = Opts,
                    url = StorageURL,
                    name = Name}.

new_blob(#remote_storage{url=URL, conn_options=Opts}, BlobRef) ->
    case coffercli_util:validate_ref(BlobRef) of
        ok ->
            URL1 = iolist_to_binary([URL, "/", BlobRef]),
            Headers = [{<<"Content-Type">>, <<"data/octet-stream">>},
                       {<<"Transfer-Encoding">>, <<"chunked">>}],
            lager:info("create ~p on ~p~n", [BlobRef, URL1]),
            hackney:request(put, URL1, Headers, stream, Opts);
        error ->
            {error, invalid_blobref}
    end.

store(eob, Client) ->
    {ok, Client1} = hackney:end_stream_request_body(Client),
    case hackney:start_response(Client1) of
        {ok, 201, _Headers, Client2} ->
            {ok, JsonBin, _} = hackney:body(Client2),
            JsonObj = jsx:decode(JsonBin),
            lager:info("got ~p~n", [JsonObj]),
            [Received] = proplists:get_value(<<"received">>, JsonObj),
            {ok, parse_blob_info(Received)};

        {ok, Status, Headers, Client2} ->
            {ok, RespBody, _} = hackney:body(Client2),
            case Status of
                405 ->
                    {error, method_not_allowed};
                404 ->
                    {error, not_found};
                _ ->
                    {error, {http_error, Status, Headers,
                             RespBody}}
            end;
        Error ->
            Error
    end;
store(Data, Client) ->
    case hackney:stream_request_body(Data, Client) of
        {ok, Client1} ->
            {ok, Client1};
        Error ->
            Error
    end.

parse_blob_info(Received) ->
    BlobRef = proplists:get_value(<<"blobref">>, Received),
    Size = proplists:get_value(<<"size">>, Received),
    {BlobRef, Size}.
