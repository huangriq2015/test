%% ============================================================================
%% Author            :huangriq - huangriq2015@163.com
%% QQ                :466796514
%% Last modified     :2017-06-24 16:18:38
%% Filename          :*********
%% Description       :************************
%% ============================================================================
-module(tcp_accept_sup).

-behaviour(supervisor).

-export([
    join_sup/1,
    start/0,
    init/1
    ]).

join_sup(Sup) ->
  supervisor:start_child(Sup, {?MODULE, staart, []}, transient, infinity, supervisor, [?MODULE]).


start() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
  Strategy = {simple_one_for_one, 10, 10},
  ChildSpec = {tcp_accept, {tcp_accept, start ,[]}, transient, 5000, worker, [tcp_accept]},
  {ok, {Strategy, [ChildSpec]}}.
