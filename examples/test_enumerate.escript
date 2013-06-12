#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./ebin

-export([main/1]).


main([]) ->
    coffercli:start(),
    Conn = coffercli:new_connection("http://localhost:8080"),
    Storage = coffercli:storage(Conn, <<"default">>),
    All = coffercli:enumerate_all(Storage),
    io:format("all:~n~n~p~n", [All]).

