%% ============================================================================
%% Author            :huangriq - huangriq2015@163.com
%% QQ                :466796514
%% Last modified     :2017-06-24 16:17:53
%% Filename          :tcp_accept
%% Description       :************************
%% ============================================================================
-module(tcp_accept).

-behaviour(gen_server).

-export([
    join_sup/5,

    start/4,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
    ]).

-include("tcp.hrl").
-include("stdin.hrl").

-record(state, {
    sock = none,
    ip  = "127.0.0.1",
    port = 0,
    ref = undefined,
    callback = undefined,
    accept_sock = none
    }).

join_sup(Sup, LSock, IP, Port, Callback) ->
  supervisor:start_child(Sup, {?MODULE, start, [LSock, IP, Port, Callback]},transient, infinity, supervisor, [?MODULE]).

start(LSock, IP, Port, Callback) ->
  gen_server:start_link(?MODULE, [LSock, IP, Port, Callback], []).

init([LSock, IP, Port, Callback]) ->
  stdlib:process_tag(?MODULE),
  self() ! accept,
  {ok, #state{sock = LSock, ip = IP, port = Port, callback = Callback}}.

handle_call({'EXIT', Reason}, _From, State) ->
  {stop, Reason, State};

handle_call(Msg, From, State) ->
  ?WARN("UNKOWN-CALL[~w]:~w", [From, Msg]),
  {reply, ok, State}.

handle_cast(Msg, State) ->
  ?WARN("UNKOWN-CAST:~w", [Msg]),
  {noreply, State}.

handle_info(accept, State) ->
  accept(State);

handle_info({inet_async, LSock, Ref, {ok, Sock}}, #state{sock = LSock, ref = Ref, ip = IP, port = Port} = State) ->
  case catch add_sock(LSock, Sock, State) of
    ok ->
      ignore;
    {error, Err} ->
      ?ERROR("~w:~w add_sock:~w,~w", [IP, Port, Sock, Err]),
      gen_tcp:close(Sock)
  end,
  accept(State#state{accept_sock = Sock});

handle_info({inet_async, _LSock, _Ref, {error, Err}}, #state{ip = IP, port = Port} = State) ->
  ?WARN("~w:~w accept fail:~w", [IP, Port, Err]),
  {noreply, State};


handle_info({func, {M, F, A}}, State) ->
  Res = ?CATCH(apply(M, F, A)),
  ?WARN("func:~w,~w,~w, res = ~w", [M, F, A, Res]),
  {noreply, State};

handle_info({'EXIT', Reason}, State) ->
  {stop, Reason, State};

handle_info({'EXIT', _PID, Reason}, State) ->
  #state{accept_sock = Sock} = State,
  ?ERROR("转移控制权出错：~w,~w,~w", [_PID, Sock, Reason]),
  {noreply, State};

handle_info(Msg, State) ->
  ?WARN("UNKOWN-INFO:~w", [Msg]),
  {noreply, State}.

terminate(Reason, State) ->
  #state{ip = IP, port = Port} = State,
  case Reason =:= normal orelse Reason =:= shutdown of
    true ->
      ignore;
    false ->
      ?ERROR("~w:~w accept exit, ~w", [IP, Port, Reason]) 
  end,
  ok.

code_change(OldVsn, Extra, State) ->
  ?WARN("code_change:~w, ~w", [OldVsn, Extra]),
  {ok, State}.

accept(#state{ip = IP, port = Port, sock = Sock} = State) ->
  case prim_inet:async_accept(Sock, -1) of
    {ok, Ref} ->
      {noreply, State#state{ref = Ref}};
    {error, Err} ->
      ?ERROR("~s:~w accept exception:~w", [IP, Port, Err]),
      timer:sleep(500),
      self() ! accept,
      {noreply, State}
  end.


add_sock(LSock, Sock, State) ->
  {ok, InetMod} = inet_db:lookup_socket(LSock),
  inet_db:register_socket(Sock, InetMod),

  #state{callback = Callback} = State,
  case inet:setopts(Sock, ?TCP_CONNECT_OPTIONS) of
    ok ->
      ignore;
    Err1 ->
      throw({error, Err1})
  end,
  case supervisor:start_child(tcp_rw_sup, [Sock, Callback, undefined]) of
    {ok, PID} ->
      gen_tcp:controlling_process(Sock, PID);
    Err2 ->
      throw({error, Err2})
  end,
  ok.
