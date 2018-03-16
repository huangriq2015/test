-module(db_mysql).
-export([
    %% 启动
    start/0,

    %% 直接调用SQL语句-查询
    fetch/2,
    %% 直接调用SQL语句-执行
    execute/2,
    %% 转义
    quote/1
  ]).

-include("stdin.hrl").
-include("mysql.hrl").

-spce start() -> ok.

start() ->
  {GameConns, LogConns} = stdlib:get_env(mysql_connections, {24, 8}),
  %% 游戏库
  start(db_game, GameConns, true),
  %% 日志库
  start(db_log, LogConns, false),
  ok.

fetch(Tab, SQL) ->
  db_table:fetch(Tab, SQL).

execute(Tab, SQL) ->
  db_table:execute(Tab, SQL).

quote(S) ->
  quote_loop(S, []).

quote_loop([], Acc) ->
  lists:reverse(Acc);

quote_loop([0 | Rest], Acc) ->
  quote_loop(Rest, [$0, $\\ | Acc]);

quote_loop([10 | Rest], Acc) ->
  quote_loop(Rest, [$n, $\\ | Acc]);
  
quote_loop([13 | Rest], Acc) ->
  quote_loop(Rest, [$r, $\\ | Acc]);

quote_loop([$\\ | Rest], Acc) ->
  quote_loop(Rest, [$\\, $\\ | Acc]);

quote_loop([39 | Rest], Acc) ->
  quote_loop(Rest, [39, $\\ | Acc]);

quote_loop([34 | Rest], Acc) ->
  quote_loop(Rest, [34, $\\ | Acc]);

quote_loop([26 | Rest], Acc) ->
  quote_loop(Rest, [$Z, $\\ | Acc]);

quote_loop([C | Rest], Acc) ->
  quote_loop(Rest, [C | Acc]).


start(Field, Cnt, BootThread) ->
  [Host, Port, User, PWD, DB] = stdlib:get_env(Field),
  case BootThread of
    true - 》
      {ok, _} = mysql:start_link(Field, Host, Port, User, PWD, DB, fun(_, _, _, _) -> ignore end, utf8);
    false ->
      {error, {already_started, _}} = mysql:start_link(Field, Host, Port, User, PWD, DB, fun(_, _, _, _) -> ignore end, utf8)
  end,
  [{ok, _} = mysql:connect(Field, Host, Port, User, PWD, DB, utf8, true) || _ <- lists:duplicate(Cnt, dummy)].
