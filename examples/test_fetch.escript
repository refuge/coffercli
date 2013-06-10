#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./ebin

-export([main/1, stream_all/2]).

stream_all(Fetcher, Bin) ->
    case coffercli:fetch(Fetcher) of 
        {ok, Data, Fetcher1} ->
            stream_all(Fetcher1, << Bin/binary, Data/binary >>);
        eob ->
            Bin;
        Error ->
            Error
    end.



main([]) ->
    coffercli:start(),
    Conn = coffercli:new_connection("http://localhost:8080"),
    Storage = coffercli:storage(Conn, <<"default">>),
    coffercli:upload(Storage, {file, "/etc/hosts"}),

    {ok, BlobRef} = coffercli_util:hash({file, "/etc/hosts"}),
    
    {ok, Bin} = coffercli:fetch_all(Storage, BlobRef),
    io:format("/etc/hosts:~n~n~s~n", [Bin]),

    {ok, Fetcher} = coffercli:fetch_init(Storage, BlobRef),
    
    %% test streaming API
    Bin1 = stream_all(Fetcher, <<>>),
    io:format("streamed /etc/hosts:~n~n~s~n", [Bin1]).

