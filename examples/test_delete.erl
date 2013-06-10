#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./ebin

-export([main/1]).

main([]) ->
    coffercli:start(),
    Conn = coffercli:new_connection("http://localhost:8080"),
    Storage = coffercli:storage(Conn, <<"default">>),
    coffercli:upload(Storage, <<"testdelete">>),

    {ok, BlobRef} = coffercli_util:hash(<<"testdelete">>),

    {ok, Bin} = coffercli:fetch_all(Storage, BlobRef),
    io:format("content uploaded: ~s~n", [Bin]),

    ok = coffercli:delete(Storage, BlobRef),
    Resp =  coffercli:fetch_all(Storage, BlobRef),


    io:format("not found?: ~p~n", [Resp]).

