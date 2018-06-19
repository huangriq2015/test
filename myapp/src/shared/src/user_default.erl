-module(user_default).

-compile([export_all]).

pid(RoleID) ->
  stdin:whereis(stdlib:role_procname(RoleID).

eprof(PID) ->
  eprof(PID, 30).

eprof(PID, Sec) ->
  eprof(PID, Sec, undefined).

eprof(PID, Sec, File) when is_pid(PID) ->
  eprof([PID], Sec, File);

eprof(PIDS, Sec, File) ->
  catch eprof:stop(),
  eprof:start_profiling(PIDS),
  timer:sleep(Sec * 1000),
  eprof:stop_profiling(),
  File =/= undefined andalso eprof:log(File),

  eprof:analyze(total, [{sort, time}]),
  eprof:stop().

stop() ->
  application:stop(game),
  init:stop().
