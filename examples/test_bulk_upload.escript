#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./ebin

-export([main/1]).


main([]) ->
    coffercli:start(),
    Conn = coffercli:new_connection("http://localhost:8080"),
    Storage = coffercli:storage(Conn, <<"default">>),

    {ok, UploadCtx} = coffercli:bulk_upload_init(Storage),

    {ok, UploadCtx1} = coffercli:bulk_upload_send(UploadCtx,
                                                  <<"hello mp world">>),

    {ok, UploadCtx2} = coffercli:bulk_upload_send(UploadCtx1,
                                                  {stream, <<"hashtype-mptest6">>}),
    {ok, UploadCtx3} = coffercli:bulk_upload_send(UploadCtx2,
                                                  <<"hello mp world\n">>),
    {ok, UploadCtx4} = coffercli:bulk_upload_send(UploadCtx3,
                                                  <<"welcome on earth\n">>),
    {ok, UploadCtx5} = coffercli:bulk_upload_send(UploadCtx4, eob),


    {ok, UploadCtx6} = coffercli:bulk_upload_send(UploadCtx5, {file, "/etc/hosts"}),
    Resp = coffercli:bulk_upload_final(UploadCtx6),
    io:format("got ~p~n", [Resp]).

