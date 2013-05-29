%%% -*- erlang -*-
%%%
%%% This file is part of coffercli released under the Apache license 2.
%%% See the NOTICE for more information.
%%%
-module(coffercli_util).

-export([request/4, request/5, request/6]).

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
                        _ ->
                            {error, {http_error, Status, RespHeaders,
                                     RespBody}}
                    end
            end;
        Error ->
            Error
    end.
