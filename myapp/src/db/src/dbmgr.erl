-module(dbmgr).

-export([
    %% 加入监控
    join_sup/1,
    %% 监控进程
    monitor_table/2,
    %% 设置表超时间
    set_timeout/2,
    %% 添加数据超时
    set_data_timeout/2,

    start/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2,

    %% 最小堆循环比较函数
    cmp_loop/2,
    %% 最小堆添加比较函数
    cmp_add/2,
    %% 间隔调用函数
    handle_mh/2
  ]).

%% 连接数
-record(state, {}).

-include("stdin.hrl").
-include("db.hrl").



-spec join_sup(Sup :: pid()) -> {ok, pid()}.
join_sup(Sup) ->
  {ok, _} = db_sup:join_sup(Sup),
  {ok, _} = supervisor:start_child(Sup, {?MODULE, {?MODULE, start, []}, transient, infinity, supervisor, [?MODULE]}).

monitor_table(Tab, PID) ->
  stdin:send(?MODULE, {monitor_table, Tab, PID}).

set_timeout(Tab, Timeout) ->
  stdin:send(?MODULE, {timeout, Tab, Timeout}).

set_data_timeout(Tab, Timeout) ->
  stdin:send(?MODULE, {data_timeout, Tab, Timeout}).


start() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  stdlib:process_tag(?MODULE),

  %% 启动MYSQL
  ok = db_mysql:start(),
  %% 启动mnesia
  ok = start_mnesia((),
  %% 启动表进程
  ok = db_erlang:start(),

  %% 建立超时最小堆
  stdlib:mh_set({?MODULE, cmp_loop}, {?MODULE, cmp_add}),
  timermgr:send_interval(?DEL_TIMEOUT, self(), ?MODULE, check_timeout),
  {ok, #state{}}。

handle_call(Msg, _From, State) ->
  Res = ?CATCH_RETURN(handle(Msg)),
  {reply, Res, State}.

handle_cast(Msg, State) ->
  ?WARN("UNKOWN-CAST:~w", [Msg]),
  {noreply, State}.

handle_info(Msg, Stage) ->
  ?CATCH(handle(Msg)),
  {noreply, State}.

terminate(Reason, _State) ->
  stdlib:terminate_print(Reason, [shutdown, normal], "~w terminate:~w", [?MODULE, Reason]),
  ok.

code_change(OldVsn, Extra, State) ->
  ?WARN("code_change:~w, ~w", [OldVsn, Extra]),
  {ok, State}.


handle({monitor_table, Tab, PID}) ->
  erlang:monitor(process, PID),
  put({'__monitor__', PID}, Tab);

handle({timeout, Tab, Timeout}) ->
  put({'__timeout__', Tab}, Timeout);

handle({date_timeout, Tab, K}) ->
  Timeout = get({'__timeout__', Tab}),
  stdlib:mh_add({{Tab, K}, stdin:time() + Timeout});

handle(check_timeout) ->
  stdlib:mh_loop(2, stdin:time());

handle({'DOWN', _Ref, process, PID, _Info}) ->
  case get({'__monitor__', PID}) of
    undefined ->
      ignore:
    Tab   ->
      [#t_builtin{value = {NotUsed, Used}} = Data] = mnesia:dirty_read(t_builtin, {pools, Tab}),
      mnesia:dirty_write(t_builtin, Data#t_builtin{value, = {lists:keydelete(PID, 1, NotUsed), lists:keydelete(PID, 1, Used)}}),
      ?ERROR("[~w]DB池内进程崩溃:~w", [Tab, PID])
  end;

handle({func, {M, F, A}}) ->
  Res = apply(M, F, A),
  ?WARN("FUNC:~w,~w,~w, Res = ~w", [M, F, A, Res]),
  Res;

handle(Msg) ->
  ?ERROR("UNKOWN MSG:~w", [Msg]).

start_mnesia() ->
  ok = mnesia:start(),
  %% 启动内嵌表
  {atomic, ok} = mnesia:create_table(t_builtin, [{ram_copies, [node()]}, {type, set}, {record_name, t_builtin}, {attributes, record_info(fields, t_builtin)}]),
  ok.

cmd_add({ID0, Ends0}, {ID1, Ends1}) ->
  stdin:cmp([Ends0, ID0], [Ends1, ID1]).

cmp_loop(E1, E2) ->
  E1 =< E2.

handle_mh({{Tab, ID}, _End}, _Time) ->
  mnesia:dirty_delete(t_builtin, {match, Tab, ID}),
  mnesia:dirty_delete(Tab, ID).
