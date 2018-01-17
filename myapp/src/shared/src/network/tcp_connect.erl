%% ============================================================================
%% Author            :huangriq - huangriq2015@163.com
%% QQ                :466796514
%% Last modified     :2017-06-26 16:38:44
%% Filename          :*********
%% Description       :************************
%% ============================================================================

-module(tcp_connect).

-behaviour(gen_server).

-export([
    %% 加入监控
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
    ip        = "127.0.0.1"       :: inet:ip_address() | inet:hostname(),
    port      = 0                 :: non_neg_integer(),
    callback  = undefined         :: reference()
    }).

-spec join_sup(Sup :: pid(), IP :: term(), Port :: inet:port_integer(), Cnt :: non_neg_integer(), Callback :: module()) -> {ok, pid()}.
join_sup(Sup, IP, Port, Cnt, Callback) ->
  supervisor:start_child(Sup, {?MODULE, {?MODULE, start, [IP, Port, Cnt, Callback]}, transient, infinity, supervisor, [?MODULE]}).

-spec start(IP :: term(), Port :: inet:port_integer(), Cnt :: non_neg_integer(), Callback :: module()) -> {ok, pid()}.
start(IP, Port, Cnt, Callback) ->
  gen_server:start_link(?MODULE, [stdin:tou(IP), Port, Cnt, Callback], []).

init([IP, Port, Cnt, Callback]) ->
  stdlib:process_tag(?MODULE),
  [async_connect(IP, Port, X) || X <- lists:seq(1, Cnt)],
  {ok, #state{ip = IP, port = Port, callback = Callback}}.

handle_call(Msg, From , State) ->
  ?WARN("UNKOWN-CALL[~w]:~w", [From, Msg]),
  {reply, ok, State}.

handle_cast(Msg, State) ->
  ?WARN("UNKOWN-CAST:~w", [Msg]),
  {noreply, State}.

handle_info({add, ID}, #state{ip = IP, port = Port} = State) ->
  async_connect(IP, Port, ID),
  {noreply, State};

handle_info({inet_async, Sock, _Ref, ok}, #state{ip = IP, port = Port, callback = Callback} = State) ->
  ?INFO("~w:~w connect setup:~w", [IP, Port, Sock]),
  ID = del_sock_id(Sock),
  case supervisor:start_child(tcp_rw_sup, [Sock, Callback, ID]) of
    {ok, PID} ->
      gen_tcp:controlling_process(Sock, PID);
    Err       ->
      gen_tcp:close(Sock),
      ?WARN("[~w:~w] start child fail:~w,~w", [IP, Port, Sock, Err])
  end,
  {noreply, State};

handle_info({inet_async, Sock, _Ref, Err}, #state{ip = IP, port = Port} = State) ->
  ?WARN("~w:~w connect fail:~w", [IP, Port, Err]),
  del_sock_id(Sock),
  gen_tcp:close(Sock),
  {noreply, State};

handle_info({func, {M, F, A}}, State) ->
  Res = ?CATCH(apply(M, F, A)),
  ?WARN("func:~w,~w,~w, res = ~w", [M, F, A, Res]),
  {noreply, State};

handle_info({'EXIT', _PID, Reason}, State) ->
  {stop, Reason, State};

handle_info(Msg, State) ->
  ?WARN("UNKOWN-INFO:~w", [Msg]),
  {noreply, State}.

terminate(Err, State) ->
  #state{ip = IP, port = Port} = State,
  ?ERROR("~w:~w connect exit, ~w", [IP, Port, Err]),
  ok.

code_change(OldVsn, Extra, State) ->
  ?WARN("code_change:~w, ~w", [OldVsn, Extra]),
  {ok, State}.


async_connect(IP, Port, ID) ->
  case inet:connect_options(?TCP_CONNECT_OPTIONS, inet) of
    {ok, {_, LocalIP, LocalPort, FD, Opts}} ->
      ignore;
    Err0 ->
      FD = undefined, LocalIP = undefined, LocalPort = undefined, Opts = undefined,
      throw(Err0)
  end,
  case inet:open(FD, LocalIP, LocalPort, Opts, tcp, inet, stream, inet_tcp) of
    {ok, Sock} ->
      ignore;
    Err1 ->
      Sock = undefined,
      throw(Err1)
  end,
  case prim_inet:async_connect(Sock, IP, Port, -1) of
    {ok, Sock, Ref} ->
      set_sock_id(Sock, ID),
      {ok, Sock, Ref};
    Err2 ->
      gen_tcp:close(Sock),
      Err2
  end.

set_sock_id(Sock, ID) ->
  put({'__sock_id__', Sock}, ID).

del_sock_id(Sock) ->
  erase({'__sock_id__', Sock}).
