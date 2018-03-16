%% ============================================================================
%% Author            
%% QQ                :
%% Last modified     :
%% Filename          :*********
%% Description       :************************
%% ============================================================================
-module(timermgr).

-export([
    join_sup/1,
    start/0,

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2,

    cmp/2,

    %% 定时发送
    send_interval/3,
    send_interval/4,
    send_after/3,
    send_after/4
  ]).

-include("stdin.hrl").

-define(LOOP_MS, 4).

-record(b_timer, {ref, pids, type, intvl, timeout, msg}).

-record(state, {}).

join_sup(Sup) ->
  supervisor:start_child(Sup, {?MODULE, {?MODULE, start, []}, transient, infinity, worker, [?MODULE]}).

start() ->
  {ok, _} = gen_server:start_link({local, ?MODULE}, ?MODULE, [], [{spawn_opt, [{priority, high}]}]).

send_interval(MS, PID, Msg) ->
  stdin:send(?MODULE, {send_interval, MS, PID, undefined, Msg}).

send_interval(MS, PID, Ref, Msg) ->
  stdin:send(?MODULE, {send_interval, MS, PID, Ref, Msg}).

send_after(MS, PID, Msg) ->
  stdin:send(?MODULE, {send_after, MS, PID, undefined, Msg}).

send_after(MS, PID, Ref, Msg) ->
  stdin:send(?MODULE, {send_after, MS, PID, Ref, Msg}).

init([]) ->
  stdlib:process_tag(?MODULE),

  init_data(),
  {ok, #state{}, timeout()}.

handle_call(Msg, _From, State) ->
  Res = ?CATCH_RETURN(handle(Msg)),
  {reply, Res, State, timeout()}.

handle_cast(Msg, State) ->
  ?WARN("UNKOWN-CAST:~w", [Msg]),
  {noreply, State, timeout()}.

%% 进程退出
handle_info({'EXIT', Reason}, State) ->
  {stop, Reason, State};

handle_info({'EXIT', PID, _Reason}, State) ->
  del_pid(PID),
  {noreply, State, timeout()};

handle_info(Msg, State) ->
  ?CATCH(handle(Msg)),
  {noreply, State, timeout()}.

terminate(Reason, _State) ->
  stdlib:terminate_print(Reason, [normal, shutdown], "terminate:~w", [Reason]),
  ok.

code_change(OldVsn, Extra, State) ->
  ?WARN("CODE_CHANGE:~w, ~w", [OldVsn, Extra]),
  {ok, State}.

init_data() ->
  init_mh(),
  ok.

handle({func, {M, F, A}}) ->
  Res = apply(M, F, A),
  ?WARN("FUNC:~w,~w,~w, Res = ~w", [M, F, A, Res]);

handle(timeout) ->
  MS = stdin:mtime(),
  handle_mh(MS),

  stdin:cancel_timer(get_timer()),
  TimerRef = erlang:send_after(timeout(), self(), timeout),
  set_timer(TimerRef);

handle({send_interval, Intvl, PID, Ref, Msg}) ->
  add_mh_intvl(send_interval, Intvl, PID, Ref, Msg);

handle({send_after, Intvl, PID, Ref, Msg}) ->
  add_mh_intvl(send_after, Intvl, PID, Ref, Msg);

handle(Msg) ->
  ?ERROR("UNKOWN MSG:~w", [Msg]).

init_mh() ->
  Mh = minheap:new({?MODULE, cmp}, [{keypos, #b_timer.ref}]),
  set_mh(Mh).

set_mh(V) ->
  put('__timer_mh__', V).

get_mh() ->
  get('__timer_mh__').

add_mh_intvl(Type, Intvl, PID, Ref, Msg) ->
  catch link(PID),

  MS = stdin:time(),
  Mh = get_mh(),
  case Ref of
    undefined ->
      T = #b_timer{ref = make_ref(), pids = [PID], type = Type, intvl = Intvl, timeout = MS + Intvl, msg = Msg};
    _         ->
      case minheap:find(Ref, Mh) of
        error ->
          T = #b_timer{ref = Ref, pids = [PID], type = Type, intvl = Intvl, timeout = MS + Intvl, msg = Msg};
        {ok, E} ->
          #b_timer{pids = PIDL0} = E,
          T = E#b_timer{pids = [PID|PIDL0]}
      end
  end,
  set_pid_ref(PID, T#b_timer.ref),
  set_mh(minheap:push(T, Mh)).

cmp(#b_timer{timeout = Timeout1}, #b_timer{timeout = Timeout2}) ->
  Timeout1 =< Timeout2.

handle_mh(MS) ->
  Mh = get_mh(),
  case minheap:size(Mh) =:= 0 of
    true ->
      ignore;
    false ->
      Top = minheap:top(Mh),
      Mh1 = minheap:pop(Mh),
      #b_timer{timeout = Timeout, pids = PIDL0, type = Type, intvl = Intvl, msg = Msg} = Top,
      case Timeout =< MS of
        true ->
          case Type of
            send_interval ->
              set_mh(minheap:push(Top#b_timer{timeout = MS + Intvl}, Mh1));
            _             ->
              set_mh(Mh1)
          end,
          MS - Timeout > 20 andalso ?ERROR("投放消息延迟:~w,~w,~w,~w", [MS - Timeout, Timeout, MS, Top#b_timer.ref]),
          [X ! Msg || X <- PIDL0],
          handle_mh(MS);
        false ->
          ignore
      end
  end.

del_pid(PID) ->
  Ref = get_pid_ref(PID),

  Mh = get_mh(),
  case minheap:find(Ref, Mh) of
    error ->
      ignore;
    {ok, E} ->
      #b_timer{pids = PIDL0} = E,
      case lists:delete(PID, PIDL0) of
        [] ->
          set_mh(minheap:remove(Ref, Mh));
        PIDL1 ->
          set_mh(minheap:push(E#b_timer{pids = PIDL1}, Mh))
      end
  end,
  ok.

timeout() ->
  Mh = get_mh(),
  case minheap:size(Mh) =:= 0 of
    true ->
      infinity;
    false ->
      Top = minheap:top(Mh),
      stdin:max(Top#b_timer.timeout - stdin:mtime() -1, 0)
  end.

set_timer(V) ->
  put('__timer__', V).

get_timer() ->
  get('__timer__').

set_pid_ref(PID, Ref) ->
  put({'__ref__', PID}, Ref).

get_pid_ref(PID) ->
  erase({'__ref__', PID}).
