-module(coffercli_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).
-export([ensure_deps_started/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    coffercli_sup:start_link().

stop(_State) ->
    ok.


ensure_deps_started() ->
    {ok, Deps} = application:get_key(coffercli, applications),
    true = lists:all(fun ensure_started/1, Deps).

ensure_started(App) ->
    case application:start(App) of
        ok ->
            true;
        {error, {already_started, App}} ->
            true;
        Else ->
            error_logger:error_msg("Couldn't start ~p: ~p", [App, Else]),
            Else
    end.


