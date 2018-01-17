%% ============================================================================
%% Author            :huangriq - huangriq2015@163.com
%% QQ                :466796514
%% Last modified     :2017-06-26 15:10:54
%% Filename          :*********
%% Description       :************************
%% ============================================================================

-module(tcp_listen).

-behaviour(gen_server).

-export([
    %% 加入监控
    join_sup/3,
    join_sup/4,
    join_sup/5,
    start/2,
    start/3,
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

%% 接受SOCKET进程数量
-define(ACCEPTOR_CNT,         16).

-record(state, {
    sock      =none           :: port(),
    ip        ="127.0.0.1"    :: inet:ip_address() | inet:hostname(),
    port      = 0             :: non_neg_integer(),
    callback  = undefined     :: reference()
    }).

-spec join_sup(Sup :: pid(), Port :: inet:port_integer(), Callback :: module()) -> {ok, pid()}.
join_sup(Sup, Port, Callback) ->
  join_sup(Sup, "127.0.0.1", Port, Callback).

-spec join_sup(Sup :: pid(), IP :: term(), Port :: inet:port_integer(), Callback :: module()) -> {ok, pid()}.
join_sup(Sup, IP, Port, Callback) ->
  join_sup(Sup, IP, Port, ?ACCEPTOR_CNT, Callback).

-spec join_sup(Sup :: pid(), IP :: term(), Port :: init:port_integer(), Cnt :: integer(), Callback :: module()) -> {ok, pid()}.
join_sup(Sup, IP, Port, Cnt, Callback) ->
  {ok, _} = tcp_accept_sup:join_sup(Sup),
supervisor:start_child(Sup, {?MODULE, {?MODULE, start, [IP, Port, Cnt, Callback]}, transient, infinity, supervisor, [?MODULE]}).

-spec start(Port :: inet:port_integer(), Callback :: module()) -> {ok, pid()}.
start(Port, Callback) ->
  start({0, 0, 0, 0}, Port, ?ACCEPTOR_CNT, Callback).

-spec start(IP :: term(), Port::inet:port_integer(), Callback :: module()) -> {ok, pid()}.
start(IP, Port, Callback) ->
  start(IP, Port, ?ACCEPTOR_CNT, Callback).

-spec start(IP :: term(), Port :: inet:port_integer(), Cnt :: integer(), Callback :: module()) -> {ok, pid()}.
start(IP, Port, Cnt, Callback) ->
  gen_server:start_link(?MODULE, [IP, Port, Cnt, Callback], []).


init([IP, Port, Cnt, Callback]) ->
  stdlib:process_tag(?MODULE),
  Options = case IP of
    {0, 0, 0, 0} ->
      ?TCP_LISTEN_OPTIONS;
    IP          ->
      [{ip, stdin:tou(IP)} | ?TCP_LISTEN_OPTIONS]
  end,
  case gen_tcp:listen(Port, Options) of
    {ok, LSock} ->
      ok = add_acceptor(LSock, IP, Port, Callback, Cnt),
      {ok, #state{sock = LSock, ip = IP, port = Port, callback = Callback}};
    {error, Err} ->
      {stop, {error, cannot_listen, IP, Port, Err}}
  end.

handle_call(Msg, From, State) ->
  ?WARN("UNKOWN-CALL[~w]:~w", [From, Msg]),
  {reply, ok, State}.

handle_cast(Msg, State) ->
  ?WARN("UNKOWN-CAST:~w", [Msg]),
  {noreply, State}.

handle_info({add_acceptor, Cnt}, State) ->
  #state{sock = LSock, ip = IP, port = Port, callback = Callback} = State,
  add_acceptor(LSock, IP, Port, Callback, Cnt),
  {noreply, State};

%% 仅限于调试
handle_info({func, {M, F, A}}, State) ->
  Res = ?CATCH(apply(M, F, A)),
  ?WARN("FUNC:~w,~w,~w, Res = ~w", [M, F, A, Res]),
  {noreply, State};

handle_info({'EXIT', _PID, Reason}, State) ->
  {stop, Reason, State};

handle_info(Msg, State) ->
  ?WARN("UNKOWN-INFO:~w", [Msg]),
  {noreply, State}.

terminate(Err, State) ->
  #state{ip = IP, port = Port} = State,
  ?ERROR("~s:~w退出, ~w", [IP, Port, Err]),
  ok.

code_change(OldVsn, Extra, State) ->
  ?WARN("code_change:~w, ~w", [OldVsn, Extra]),
  {ok, State}.


add_acceptor(_Sock, _IP, _Port, _Callback, 0) ->
  ok;
add_acceptor(Sock, IP, Port, Callback, N) ->
  case supervisor:start_child(tcp_accept_sup, [Sock, IP, Port, Callback]) of
    {ok, _} ->
      add_acceptor(Sock, IP, Port, Callback, N - 1);
    _Err ->
      _Err
  end.
