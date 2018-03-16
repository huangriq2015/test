-module(db_sup).

-behaviour(supervisor).

-export([
  %% 加入监控
  join_sup/1,
  start/1,
  init/1
  ]).

-spec join_sup(Sup :: pid()) -> {ok, pid()}.

join_sup(Sup) ->
  supervisor:start_child(Sup, {?MODULE, {?MODULE, start, []}, transient, infinity, supervisor, [?MODULE]}).

start() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  Strategy = {one_for_one, 5, 10},
  {ok, {Strategy, []}}.
