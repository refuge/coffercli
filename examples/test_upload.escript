#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./ebin

-export([main/1]).


main([]) ->
    coffercli:start(),
    Conn = coffercli:new_connection("http://localhost:8080"),
    Storage = coffercli:storage(Conn, <<"default">>),
    Resp = coffercli:upload(Storage, <<"hello world">>),
    io:format("got ~p~n", [Resp]),

    {ok, Client2} = coffercli:upload(Storage, {stream,
                                               <<"hashtype-20130608test10">>}),
    {ok, Client3} = coffercli:send_blob_part(Client2, <<"hello world\n">>),
    {ok, Client4} = coffercli:send_blob_part(Client3, <<"welcome on earth\n">>),
    Resp1 = coffercli:send_blob_part(Client4, eob),

    io:format("got ~p~n", [Resp1]),
    Resp2 = coffercli:upload(Storage, {file, "/etc/hosts"}),
    io:format("got ~p~n", [Resp2]).

