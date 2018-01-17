-module(myapp_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  {ok, Sup} = myapp_sup:start_link(),

  {ok, _} = tcp_rw_sup:join_sup(Sup),
  {ok, _} = tcp_listen:join_sup(Sup, "127.0.0.1", 443, gate),
  {ok, Sup}.

stop(_State) ->
    ok.
