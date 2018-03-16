-module(db_erlang).
-compile(export_all).

%% 库 表 日志 预加载 超时 日志自增量
addto_table(DB, Tab, Log, Load, Timeout, SQLID) ->
  %% 表信息
  ?ERROR("初始化表结构:~w", [Tab]),
  TableInfo = #table_info{db = DB, fields = fields(Tab), join_fields = join_fields(fields(Tab)), sql_id = SQLID},
  case catch db_table:check_sql(TableInfo) of
    ok ->
      ignore;
    Err ->
      ?ERROR("初始化表结构失败:~w,~w" [Tab, Err]),
      throw(Err)
  end,
  mnesia:dirty_write(t_builtin, #t_builtin{key = {record, Tab}, value = TableInfo}),
  %% 日志&池&超时
  case Log of
    true ->
      mnesia:dirty_write(t_builtin, #t_builtin{key = {log, Tab}, value = true}),
      mnesia:dirty_write(t_builtin, #t_builtin{key = {pools, Tab}, value = {start_table_pools(Tab), []}});
    false ->
      dbmgr:set_timeout(Tab, Timeout)
  end,
  %% 预加载
  case Load of
    true ->
      mnesia:dirty_write(t_builtin, #t_builtin{key = {load, Tab}, value = true}),
      [mnesia:dirty_write(Tab, X) || X <- db_table:match(Tab)];
    false ->
      ignore
  end,
  ok.

start_table_pools(Tab) ->
  [start_table_pools(Tab, X) || X <- lists:seq(1, ?POOL_CNT)].

start_table_pools(Tab, ID) ->
  {ok, PID} = db_table:join_sup(db_sup, #table_pools{table = Tab, id = ID}),
  dbmgr:monitor_table(Tab, PID),
  {PID, 0}.

join_fields([{_Table, Fields}|_] = AllFields) ->
  FieldL0 = lists:reverse(join_fields(Fields, AllFields, [])),
  FieldL1 = [lists:concat(["`", X, "`"]) || X <- FieldL0], 
  string:join(FieldL1, ", ").

join_fields([], _AllFields, Acc) ->
  Acc;

join_fields([{_, sql_id, _}|T], AllFields, Acc) ->
  join_fields(T, AllFields, Acc);

join_fields([{Name, Type, _}|TField], AllFields, Acc) when 
Type =:= int; Type =:=string; Type =:= mixed; Type =:= atom; Type =:= sql_time ->
  join_fields(TField, AllFields, [Name, |Acc]);

join_fields([{Name, _Record, _}|TField], AllFields, Acc) ->
  {_, Fields} = lists:keyfind(Name, 1, AllFields),
  join_fields(TField, AllFields, join_fields(Fields, AllFields, []) ++ Acc).

start() ->
  {atomic, ok} = mnesia:create_table(t_ban, [{ram_copies, [node()]}, {type, set}, {record_name, t_ban}, {attributes, record_info(fields, t_ban)}, {index, []}]),
  ok = addto_table(db_game, t_ban, false, false, 3600, false),
  ok.
