%%% -*- erlang -*-
%%%
%%% This file is part of coffercli released under the Apache license 2.
%%% See the NOTICE for more information.
%%%
-module(coffercli_util).

-export([request/4, request/5, request/6]).
-export([validate_ref/1]).
-export([hash/1]).

-define(BLOCKSIZE, 32768).

request(Method, Url, Expect, Options) ->
    request(Method, Url, Expect, Options, [], []).
request(Method, Url, Expect, Options, Headers) ->
    request(Method, Url, Expect, Options, Headers, []).
request(Method, Url, Expect, Options, Headers, Body) ->
    Accept = {<<"Accept">>, <<"application/json, */*;q=0.9">>},
    case hackney:request(Method, Url, [Accept|Headers], Body, Options) of
        {ok, Status, RespHeaders, Client} ->
            {ok, RespBody, _} = hackney:body(Client),
            case lists:member(Status, Expect) of
                true ->
                    {ok, Status, RespHeaders, RespBody};
                false ->
                    case Status of
                        405 ->
                            {error, method_not_allowed};
                        404 ->
                            {error, not_found};
                        409 ->
                            {error, already_exists};
                        _ ->
                            {error, {http_error, Status, RespHeaders,
                                     RespBody}}
                    end
            end;
        Error ->
            Error
    end.

validate_ref(Ref) ->
    Re = get_blob_regexp(),
    case re:run(Ref, Re, [{capture, none}]) of
        nomatch ->
            error;
        _ ->
            ok
    end.

get_blob_regexp() ->
    %% we cache the regexp so it can be reused in the same process
    case get(blob_regexp) of
        undefined ->
            {ok, RegExp} = re:compile("^([a-z][a-zA-Z0-9^-]*)-([a-zA-Z0-9]*)$"),
            put(blob_regexp, RegExp),
            RegExp;
        RegExp ->
            RegExp
    end.


hash({file, File}) ->
    case file:open(File, [binary,raw,read]) of
        {ok, P} ->
            loop(P, crypto:sha_init());
        Error ->
            Error
    end;
hash(Bin) when is_binary(Bin)->
    Hash = hexdigest(crypto:sha(Bin)),
    {ok, << "sha1-", Hash/binary >>}.

loop (P, C) ->
    case file:read(P, ?BLOCKSIZE) of
        {ok, Bin} ->
            loop(P, crypto:sha_update(C, Bin));
        eof ->
            file:close(P),
            Hash = hexdigest(crypto:sha_final(C)),
            {ok, << "sha1-", Hash/binary >>};
        Error ->
            Error
    end.

hexdigest(Hash) ->
    iolist_to_binary([io_lib:format("~.16b",[N]) || N
                                                 <-binary_to_list(Hash)]).
