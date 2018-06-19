-module(adm).
-export([
    process_code/1,
    load/1,
    load_module/1,
    exec/1,
    gc/1,
    update_counter/2,
    restart/0,
    scheduler/0,
    scheduler/1,


    dproc/1,
    dproc/2,

    mem/0,
    mem/2,

    msglen/0,
    msglen/2,

    msg/1,
    msg/3,

    name/1
  ]).

-include("stdin.hrl").

-spec process_code(Mod :: module()) -> [pid()].
process_code(Mod) ->
  [PID || PID <- processes(), check_process_code(PID, Mod)].

-spec load(Mod :: module() | [module()]) -> [{module(), [{node(), term()}]}].
load(Mod) when is_atom(Mod) ->
  {GoodNodes, BadNodes} = rpc:multicall(adm, load_module, [Mod]),
  L0 = [{Node, disconnect} || Node <- BadNodes],
  L1 = [{Node, Err} || {Node, {error, Err}} <- GoodNodes],
  {Mod, L0 ++ L1};
load(Mods) ->
  ResL0 = [load(Mod) || Mod <- Mods],
  case [{X, Y} || {X, Y} <- ResL0, Y =/= []] of
    [] ->
      ok;
    L0 ->
      L0
  end.

-spec load_module(Mod :: module()) -> {error, old_exist} | {node(), term()}.
load_module(Mod) ->
  case code:soft_purge(Mod) of
    true ->
      Res = code:load_file(Mod);
    false ->
      Res = {error, old_exist}
  end,
  ?ERROR("热更新: ~w -> ~w", [Mod, Res]),
  {noed(), Res}.

-spec dproc(PID :: stdin:upid()) -> term().
dproc(UPID) ->
  case stdin:whereis(UPID) of
    undefined ->
      {error, undefined};
    PID ->
      {_, L0} = rpc:pinfo(PID, dictionary),
      L0
  end.

-spec dproc(PID :: stdin:upid(), KEY :: atom()) ->  trem().
dropc(UPID, Key) ->
  case stdin:whereis(UPID) of
    undefined ->
      {error, undefined;
    PID ->
      {_, L0} = rpc:pinfo(PID, dictionary),
      proplists:get_value(Key, L0)
  end.

-spec mem() -> [{pid(), integer()}].
mem() ->
  mem(-1, 1048576).

-spec mem(N :: integer(), Threshold :: integer()) -> [pid(), integer()}].
mem(N, Threshold) ->
  sublist(memory, N, Threshold).

-spec msglen() -> [{pid(), integer()}].
msglen()->
  msglen(-1, 100).
-spec msglen(N :: integer(), Threshold :: integer()) -> [{pid(), integer()}].
msglen(N, Threshold) ->
  sublist(message_queue_len, N, Threshold).

-spec msg(UPID :: stdin:upid()) -> [{term(), term()}].
msg(UPID) ->
  msg(UPID, begs, -1).
-spec msg(UPID :: stdin:upid(), Offset :: begs | ends, N :: integer()) -> [{term(), term()}] | {error, term()}.
msg(UPID, Offset, N) ->
  case stdin:whereis(UPID) of
    undefined ->
      {error, undefined};
    PID ->
      {_, L0} = rpc:pinfo(PID, messages),
      case {Offset, N} of
        {_, -1} ->
          L0;
        {ends, _} ->
          stdin:tl(L0, N);
        _ ->
          stdin:hd(L0, N)
      end
  end.

exec(F) ->
  F(),
  ok.

gc(Max) ->
  [erlang:garbage_collect(X) || X <- processes(), check_gc(X, Max)],
  erlang:garbage_collect().

update_counter(K, V) ->
  N = stdlib:get_env(K, 0),
  stdlib:set_env(K, N + V).

restart() ->
  case stdlib:get_env(version) of
    "" ->
      Version = "dev";
    Version ->
      ignore
  end,
  Result = os:cmd(lists:concat(["cd /data/dzz_bin/", Version, "; ./bootstrap restart"])),
  ?ERROR("重启服务器:~ts", [Result]).

scheduler() ->
  scheduler(18000).

scheduler(MS) ->
  erlang:system_flag(scheduler_wall_time, true),

  L0 = lists:sort(erlang:statistics(scheduler_wall_time)),
  timer:sleep(MS),
  L1 = lists:sort(erlang:statistics(scheduler_wall_time)),

  Result = lists:map(fun({{I, A0, T0}, {I, A1, T1}}) -> {I, (A1 - A0) / (T1 - T0)} end, lists:zip(L0, L1)),

  erlang:system_flag(scheduler_wall_time, false),
  Result.

name(PID) ->
  case erlang:process_info(PID, registered_name) of
    [] ->
      case globalmgr:name(PID) of
        undefined ->
          case [X || X <- global:registered_names(), global:whereis_name(X) =:= PID] of
            [] ->
              adm:dproc(PID, '$ancestors');
            Name ->
              Name
          end;
        Name ->
          Name
      end;
    {_, Name} ->
      Name
  end.

sublist(Type, N, Threshold) ->
  L0 = [{X, Val} || X <- processes(), begin {_, Val} = process_info(X, Type), Val >= Threshold end],
  L1 = lists:sort(fun({_, Val1}, {_, Val2}) -> Val1 > Val2 end, L0),
  case N of
    -1 ->
      L1;
    _ ->
      lists:sublist(L1, 1, N)
  end.

check_gc(PID, Max) ->
  case erlang:process_info(PID, status) of
    {status, waitiing} ->
      case erlang:process_info(PID, memory of
        {_, N} when N >= Max ->
          true;
        _         ->
          false
      end;
    _ ->
      false
  end.
