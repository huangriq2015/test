%% ============================================================================
%% Author            :huangriq - huangriq2015@163.com
%% QQ                :466796514
%% Last modified     :2017-06-26 16:03:44
%% Filename          :*********
%% Description       :************************
%% ============================================================================

%% 回调模块必须实现以下4个函数
%% open(Sock)             SOCKET成功
%% recv(Sock, Msg)        接收TCP数据
%% close(Sock, Msg)       SOCKET关闭
%% message(Msg)           进程其他数据

-module(tcp_rw).

-behaviour(gen_server).

-export([
    start/3,

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
  ]).

-include("stdin.hrl").

-record(state, {sock, ref, callback}).


-spec start(Sock :: port(), Callback :: atom(), UserData :: term()) -> {ok, pid()}.
start(Sock, Callback, UserData) ->
  gen_server:start_link(?MODULE, [Sock, Callback, UserData], [{spawn_opt, [{fullsweep_after, 1024}, {min_heap_size, 1024}]}]).


init([Sock, Callback, UserData]) ->
  stdlib:process_tag(?MODULE),
  self() ! async_recv,
  Callback:open(Sock, UserData),
  {ok, #state{sock = Sock, callback = Callback}}.

handle_call({'EXIT', Reason}, _From, State) ->
  {stop, Reason, State};

handle_call(Msg, _From, #state{callback = Callback} = State) ->
  Res = ?CATCH_RETURN(Callback:message_return(Msg)),
  {reply, Res, State}.

handle_cast(Msg, State) ->
  ?WARN("UNKOWN-CAST:~w", [Msg]),
  {noreply, State}.

handle_info(async_recv, State) ->
  async_recv(State);

handle_info({inet_async, Sock, Ref, {ok, Data}}, #state{sock = Sock, ref = Ref, callback = Callback} = State) ->
  ?CATCH(Callback:recv(Sock, Data)),
  async_recv(State);

handle_info({inet_async, Sock, Ref, {error, Err}}, #state{sock = Sock, ref = Ref} = State) ->
  {stop, {error, Err}, State};

handle_info({inet_reply, Sock, ok}, #state{sock = Sock} = State) ->
  {noreply, State};

handle_info({inet_reply, Sock, Err}, #state{sock = Sock} = State) ->
  {stop, Err, State};


handle_info({func, {M, F, A}}, State) ->
  Res = ?CATCH(apply(M, F, A)),
  ?WARN("func:~w,~w,~w, res = ~w", [M, F, A, Res]), 
  {noreply, State};

handle_info({'EXIT', Reason}, State) ->
  {stop, Reason, State};

handle_info({'EXIT', _PID, Reason}, State) ->
  {stop, Reason, State};

handle_info(Msg, #state{callback = Callback} = State) ->
  ?CATCH(Callback:message(Msg)),
  {noreply, State}.

terminate(Err, #state{sock = Sock, callback = Callback}) ->
  gen_tcp:close(Sock),
  Callback:close(Sock, Err),
  ok.

code_change(OldVsn, Extra, State) ->
  ?WARN("code_change:~w, ~w", [OldVsn, Extra]),
  {ok, State}.


async_recv(#state{sock = Sock} = State) ->
  case prim_inet:async_recv(Sock, 0, -1) of
    {ok, Ref} ->
      {noreply, State#state{ref = Ref}};
    Err ->
      ?ERROR("async_recv:~w", [Err]),
      {noreply, State}
  end.
