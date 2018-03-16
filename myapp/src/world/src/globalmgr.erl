%% ============================================================================
%% Author            :
%% QQ                :
%% Last modified     :
%% Filename          :*********
%% Description       :
%% ============================================================================

-module(globalmgr).

-behaviour(gen_server).

-export([
    %% 加入监控
    join_sup/1,

    %% 最小堆循环比较函数
    cmp_loop/2,
    %% 最小堆添加比较函数
    cmp_add/2,
    %% 轮询回调
    handle_mh/2,

    %% 注册名字
    register_name/2,
    %% 撤销注册名字
    unregister_name/1,
    %% 查找名字进程ID
    whereis_name/1,
    %% 注册名字列表
    registered_names/0,
    %% 进程名称
    name/1,

    %% 回调函数
    start/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
  ]).

-include("stdin.hrl").

-define(LOOP, 60000).

%% 注册设施
-record(e_global_register, {name, pid}).

-record(state, {}).

-spec join_sup(Sup :: pid()) -> {ok, pid()}.
join_sup(Sup) ->
  supervisor:start_child(Sup, {?MODULE, {?MODULE, start, []}, transient, infinity, worker, [?MODULE]}).

%% 注册设施

-spec register_name(Name :: string(), PID :: pid()) -> yes|no.
register_name(Name, PID) ->
  stdin:call(?MODULE, {register_name, Name, PID}).

-spec unregister_name(Name :: string()) -> term().
unregister_name(Name) ->
  stdin:call(?MODULE, {unregister_name, Name}).

-spec whereis_name(Name :: string()) -> undefined|pid().
whereis_name(Name) ->
  case mnesia:dirty_read(e_global_register, Name) of
    [#e_global_register{pid = PID}] ->
      PID;
    [] ->
      undefined
  end.

-spec registered_names() -> [term()].
registered_names()->
  mnesia:dirty_select(e_global_register, ets:fun2ms(fun(#e_global_register{name = X}) -> X end)).

-spec name(PID :: pid()) -> term().
name(PID) ->
  case mnesia:dirty_select(e_global_register, ets:fun2ms(fun(#e_global_register{name = X, pid = Y}) when Y =:= PID -> X end)) of
    [] ->
      undefined;
    [Name] ->
      Name;
    NameL0 ->
      NameL0
  end.


start() ->
  {ok, _} = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  stdlib:process_tag(?MODULE),
  %% 仿global全局注册名
  {atomic, ok} = mnesia:create_table(e_global_register, [{ram_copies, [node()]}, {type, set}, {record_name, e_global_register},
      {attributes, record_info(fields, e_global_register)}, {index, [pid]}]),

  %% 建立最小堆
  stdlib:mh_set({?MODULE, cmp_loop}, {?MODULE, cmp_add}),
  timermgr:send_interval(?LOOP, self(), ?MODULE, loop),
  {ok, #state{}}.

handle_call(Msg, _From, State) ->
  Res = ?CATCH_RETURN(handle(Msg)),
  {reply, Res, State}.

handle_cast(Msg, State) ->
  ?WARN("UNKOWN-CAST:~w", [Msg]),
  {noreply, State}.

%% 进程退出
handle_info({'EXIT', Reason}, State) ->
  {stop, Reason, State};

handle_info({'EXIT', _PID, Reason}, State) ->
  {stop, Reason, State};

handle_info(Msg, State) ->
  ?CATCH(handle(Msg)),
  {noreply, State}.

terminate(Reason, _State) ->
  ?ERROR("terminate:~w", [Reason]),
  ok.

code_change(OldVsn, Extra, State) ->
  ?WARN("code_change:~w, ~w", [OldVsn, Extra]),
  {ok, State}.


handle({register_name, Name, PID}) ->
  case mnesia:dirty_read(e_global_register, Name) of
    [_] ->
      no;
    [] ->
      mnesia:dirty_write(e_global_register, #e_global_register{name = Name, pid = PID}),
      monitor(process, PID),
      yes
  end;

handle({unregister_name, Name}) ->
  mnesia:dirty_delete(e_global_register, Name);

%% 被监控进程退出
handle({'DOWN', _Ref, process, PID, _Info}) ->
  L0 = mnesia:dirty_match_object(e_global_register, #e_global_register{pid = PID, _ = '_'}),
  [mnesia:dirty_delete_object(e_global_register, X) || X <- L0];

%% 循环
handle(loop) ->
  stdlib:mh_loop(2, stdin:time());

%% 仅限于调试
handle({func, {M, F, A}}) ->
  Res = apply(M, F, A),
  ?WARN("FUNC:~w,~w,~w, Res = ~w", [M, F, A, Res]),
  Res;

handle(Msg) ->
  ?ERROR("UNKOWN MSG:~w", [Msg]).

%% 添加比较函数
cmp_add({Method1, Ends1}, {Method2, Ends2}) ->
  stdin:cmp([Ends1, Method1], [Ends2, Method2]).

%% 轮询比较函数
cmp_loop(E1, E2) ->
  E1 =< E2.

handle_mh({Method, _Ends}, _Time) ->
  ?MODULE:Method().
