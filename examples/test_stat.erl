#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./ebin

-export([main/1]).

main([]) ->
    coffercli:start(),
    Conn = coffercli:new_connection("http://localhost:8080"),
    Storage = coffercli:storage(Conn, <<"default">>),
    coffercli:upload(Storage, <<"test1">>),
    coffercli:upload(Storage, <<"test2">>),

    {ok, BlobRef1} = coffercli_util:hash(<<"test1">>),
    {ok, BlobRef2} = coffercli_util:hash(<<"test2">>),

    {ok, Found, Partials} = coffercli:stat(Storage, [BlobRef1, BlobRef2]),
    io:format("founds: ~p~npartials: ~p~n", [Found, Partials]).

