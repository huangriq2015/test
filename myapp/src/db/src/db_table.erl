-module(db_table).
-export([
    join_sup/2,

    check_sql/1,
    %% 读取
    read/2,
    %% 匹配
    match/1,
    match/2,
    %% 自增量
    insert_id/1,
    %% 写入
    write/2,
    %% 更新
    update/2,
    %% 删除
    delete/2,
    delete_object/2,
    delete_match_object/2,
    %% 清表
    clear/1,
    %% 查下SQL，返回原始的SQL形式值
    fetch/2,
    fetch/3,
    %% 执行SQL
    execute/2,
    execute/3,
    %% 比较两个元素
    cmp/3,
    %% 事务
    transaction/1,
    %% 玩家ID
    set_roleid/1,
    get_roleid/0,

    start/1, 
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
  ]).

-include("stdin.hrl").
-include("mysql.hrl").
-include("db.hrl").

-record(state, {}).

-spec join_sup(Sup :: pid(), TableInfo :: #table_pools{}) -> {ok, pid()}.
join_sup(Sup, #table_pools{table = Table, id = ID} = TableInfo) ->
  {ok, _} = supervisor:start_child(Sup, {stdin:to_atom(lists:concat([Table, '_', ID])), {?MODULE, start, [TableInfo]}, transient, infinity, worker, [?MODULE]}).

insert_id(Table) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  SQL = to_sql(insert_id, TableInfo),
  [[ID]] = inl_fetch(Table, TableInfo, SQL),
  ?SEL(is_integer(ID), ID + 1, 1).

read(Table, Key) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  SQL = to_sql({key, Key}, TableInfo),
  L0 = inl_fetch(Table, TableInfo, SQL),
  [from_sql(X, TableInfo) || X <- L0].

write(Table, Data) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  SQL = to_sql({insert, Data}, TableInfo),
  inl_execute(Table, TableInfo, SQL).

update(Table, Data) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  SQL = to_sql({update, Data}, TableInfo),
  inl_execute(Table, TableInfo, SQL).


delete(Table, Key) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  SQL = to_sql({delete, Key}, TableInfo),
  inl_execute(Table, TableInfo, SQL).


delete_object(Table, Data) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  SQL = to_sql({delete_object, Data}, TableInfo),
  inl_execute(Table, TableInfo, SQL).

delete_match_object(Table, MS) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  SQL = to_sql({delete_match_object, MS}, TableInfo),
  inl_execute(Table, TableInfo, SQL).

match(Table) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  SQL = to_sql({delete, Key}, TableInfo),
  L0 = inl_fetch(Table, TableInfo, SQL, ?SELECT_TIMEOUT),
  [from_sql(X, TableInfo) || X <- L0].

match(Table, {match, MS}) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  SQL = to_sql({match, MS}, TableInfo),
  L0 = inl_fetch(Table, TableInfo, SQL, ?SELECT_TIMEOUT),
  [from_sql(X, TableInfo) || X <- L0].

clear(Table) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  SQL = to_sql(clear, TableInfo),
  inl_execute(Table, TableInfo, SQL).

fetch(Table, SQL) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  inl_fetch(Table, TableInfo, SQL).

fetch(Table, SQL, Timeout) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  inl_fetch(Table, TableInfo, SQL, Timeout).

execute(Table, SQL) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  inl_execute(Table, TableInfo, SQL).

execute(Table, SQL, Timeout) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  inl_execute(Table, TableInfo, SQL, Timeout).

print_sql(M1, SQL) ->
  Timeout = stdin:get_env(sql_timeout),
  case is_integer(Timeout) andalso stdin:mtime() - M1 >= Timeout of
    true ->
      ?ERROR("SQL:~w, ~s", [stdin:mtime() - M1, SQL]);
    false ->
      ignore
  end,
  ok.

cmp(E, E, _TableInfo) ->
  0;
cmp(E1, E2, TableInfo) ->
  cmp_sql_key(E1, E2, TableInfo).

transaction(Fun) ->
  mysql:transaction(db_game, Fun, ?TRANSACTION_TIMEOUT).

set_roleid(V) ->
  put('__roleid__', V).

get_roleid() ->
  get('__roleid__').

inl_fetch(Table, TableInfo, SQL) ->
  inl_fetch(Table, TableInfo, SQL, ?MYSQL_TIMEOUT).

inl_fetch(_Table, TableInfo, SQL, Timeout) ->
  M1 = stdin:mtime(),
  {data, #mysql_result{rows = L0} = mysql:fetch(TableInfo#table_info.db, SQL, Timeout),
  print_sql(M1, SQL),
  L0.

inl_execute(Table, TableInfo, SQL) ->
  inl_execute(Table, TableInfo, SQL, ?MYSQL_TIMEOUT).

inl_execute(_Table, TableInfo, SQL, Timeout) ->
  M1 = stdin:mtime(),
  case mysql:fetch(TableInfo#table_info.db, SQL, Timeout) of
    {updated, #mysql_result{errcode = 0}} ->
      ignore;
    {error, #mysql_result{errcode = 1062}} = Err ->
      ?ERROR("SQL重复:~w,~s,~w", [get_roleid(), SQL, Err]),
      inl_execute(_Table, TableInfo, replace(SQL));
    Err ->
      ?ERROR("SQL异常:~s,~w", [SQL, Err]),
      throw(Err)
  end,
  print_sql(M1, SQL).

start(TableInfo) ->
  get_server:start_link(?MODULE, [TableInfo], [{spawn_opt, [{fullsweep_after, 0}, {min_heap_size, 2048}]}]).

init([#table_pools{table = Table}]) ->
  stdlib:process_tag(?MODULE),

  set_table(Table),
  set_data([]),
  set_datacnt(0),

  timermgr:send_interval(?TIMEOUT, self(), ?MODULE, loop),
  {ok, #state{}}.

handle_call(Msg, _From, State) ->
  Res = ?CATCH_RETURN(handle(Msg)),
  {reply, Res, State}.

handle_cast(Msg, State) ->
  ?WARN("UNKOWN-CAST:~w", [Msg]),
  {noreply, State}.

handle_info({'EXIT', Reason}, State) ->
  {stop, Reason, State};

handle_info({'EXIT', _PID, Reason}, State) ->
  {stop, Reason, State};

handle_info(Msg, State) ->
  ?CATCH(handle(Msg)),
  {noreply, State}.

terminate(Reason, _State) ->
  case Reason =:= shutdown orelse Reason =:= normal of
    true ->
      ignore;
    false ->
      ?ERROR("~w terminate:~w", [get_table(), Reason])
  end,
  ok.

code_change(OldVsn, Extra, State) ->
  ?WARN("CODE_CHANGE:~w, ~w", [OldVsn, Extra]),
  {ok, State}.

set_datacnt(V) ->
  put('__datacnt__', V).

get_datacnt() ->
  get('__datacnt__').

set_data(V) ->
  put('__data__', V).

get_data() ->
  get('__data__').

set_table(V) ->
  put('__table__', V).

get_table() ->
  get('__table__').


handle({write, RD}) ->
  inl_update(RD),
  case get_datacnt() >= ?DATACNT of
    true ->
      dump();
    false ->
      ignore
  end;

handle(loop) ->
  dump();

handle(dump) ->
  dump();

handle({func, {M, F, A}}) ->
  Res = apply(M, F, A),
  ?WARN("FUNC:~w,~w,~w, Res = ~w", [M, F, A, Res]),
  Res;

handle(Msg) ->
  ?ERROR("UNKOWN MSG:~w", [Msg]).

dump() ->
  Table = get_table(),
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Table}),
  L0 = lists:reverse(get_data()),
  case L0 =/= [] of
    true ->
      case catch inl_write(Table, L0, TableInfo) of
        ok  ->
          set_data([]),
          set_datacnt(0);
        {error, {no_connection_in_pool, _}} ->
          ?ERROR("批量写入出错:~w, MYSQL崩溃", [Table]);
        Err ->
          ?ERROR("批量写入出错:~w, ~w", [Table, Err]),
          [?CATCH(inl_write(Table, [X], TableInfo)) || X <- L0],
          set_data([]),
          set_datacnt(0)
      end;
    false ->
      ignore
  end.

inl_update(RD) ->
  L0 = get_data(),
  set_data(RD ++ L0),
  Cnt = get_datacnt(),
  set_datacnt(length(RD) + Cnt).

inl_write(Table, Data, TableInfo) ->
  #table_info{sql_id = SQLID} = TableInfo,
  case SQLID of
    true ->
      SQL = to_sql({insert, Data}, TableInfo);
    false ->
      SQL = to_sql({replace, Data}, TableInfo)
  end,
  inl_execute(Table, TableInfo, SQL, ?BATCH_INSERT_TIMEOUT).


from_sql(SQLData, #table_info{fields = [{Table, Fields|_]} = TableInfo) ->
  from_sql(SQLData, Fields, TableInfo, [Table]).

from_sql([], [], _TableInfo, Acc) ->
  list_to_tuple(lists:reverse(Acc));

from_sql([H|T], [{_, sql_id, _}|TField], TableInfo, Acc) ->
  from_sql(T, TField, TableInfo, [H|Acc]);

from_sql([H|T], [{_, sql_time, _}|TField], TableInfo, Acc) ->
  from_sql(T, TField, TableInfo, [H|Acc]);

from_sql([H|T], [{_, int, _}|TField], TableInfo, Acc) ->
  from_sql(T, TField, TableInfo, [T|Acc]);

from_sql([H|T], [{_, string, _}|TField], TableInfo, Acc) ->
  from_sql(T, TField, TableInfo, [binary_to_list(H)|Acc]);

from_sql([H|T], [{_, mixed, _}|TField], TableInfo, Acc) ->
  Term = ?SEL(H =:= <<>>, [], stdin:to_term(binary_to_list(H))),
  from_sql(T, TField, TableInfo, [Term|Acc]);

from_sql([H|T], [{_, atom, _}|TField], TableInfo, Acc) ->
  from_sql(T, TField, TableInfo, [binary_to_atom(H, utf8)|Acc]);

from_sql(L0, [{Name, RD, _}|TField], TableInfo, Acc) ->
  {_, Fields} = lists:keyfind(Name, 1, TableInfo#table_info.fields),
  {H, T} = lists:split(length(Fields), L0),
  from_sql(T, TField, TableInfo, [from_sql(H, Fields, TableInfo, [RD])|Acc]).

to_sql(insert_id, #table_info{fields = [{Table, [{KeyName, _, _}|_]}|_]}) ->
  stdlib:format("select max(`~w`) from `~w`;", [KeyName, Table]);

to_sql({key, Key}, #table_info{fields = [{Table, [{KeyName, KeyType, _}|_]}|_]}) ->
  case KeyType =:= int orelse KeyType =:= sql_id orelse KeyType =:= sql_time of
    true ->
      stdlib:format("select * from `~w` where `~w` = ~w;", [Table, KeyName, transform(KeyType, Key)]);
    false ->
      stdlib:format("select * from `~w` where `~w` = ~s;", [Table, KeyName, transform(KeyType, Key)])
  end;

to_sql(match, #table_info{fields = [{Table, _}|_]}) ->
  stdlib:format("select * from `~w`;", [Table]);

to_sql({match, MS}, #table_info{fields = [{Table, _}|_]} = TableInfo) ->
  case string:join(gen_sql_where(MS, TableInfo), " && ") of
    "" ->
      stdlib:format("select * from `~w`;", [Table]);
    S0 ->
      stdlib:format("select * from `~w` where ~s;", [Table, S0])
  end;

to_sql({delete, Key}, #table_info{fields = [{Table, [{KeyName, KeyType, _}|_]}|_]}) ->
  case KeyType =:= int orelse KeyType =:= sql_id orelse KeyType =:= sql_time of
    true ->
      stdlib:format("delete from `~w` where `~w` = ~w;", [Table, KeyName, transform(KeyType, Key)]);
    false ->
      stdlib:format("delete from `~w` where `~w` = ~s;", [Table, KeyName, transform(KeyType, Key)])
  end;

to_sql({delete_object, RD}, #table_info{fields = [{Table, _}|_]} = TableInfo) ->
  case is_tuple(RD) of
    true ->
      S0 = gen_sql_key(RD, TableInfo);
    false ->
      S0 = string:join([gen_sql_key(X, TableInfo) || X <- RD], " || ")
  end,
  stdlib:format("delete from `~w` where ~s;", [Table, S0]);


to_sql(delete_match_object, MS}, #table_info{fields = [{Table, _}|_]} = TableInfo) ->
  case string:join(gen_sql_where(MS, TableInfo), " && ") of
    "" ->
      stdlib:format("delete from `~w`;", [Table]);
    S0 ->
      stdlib:format("delete from `~w` where ~s;", [Table, S0])
  end;

to_sql(clear, #table_info{fields = [{Table, _}|_]}) ->
  stdlib:format("delete from `~w`;", [Table]);

to_sql({update, Data}, #table_info{fields = [{Table, _}|_]} = TableInfo) ->
  {Where, Update} = gen_sql_update(Data, TableInfo),
  stdlib:format("update `~w` set ~s where ~s;", [Table, string:join(Update, ","), string:join(Where, " && ")]);

to_sql({insert, L0}, #table_info{sql_id = SQLID, fields = [{Table, _}|_], join_fields = JoinFields} = TableInfo) ->
  S0 = string:join([gen_sql_insert(X, SQLID, TableInfo) || X <- L0], ", "),
  stdlib:format("insert into ~w(~s) values ~s", [Table, JoinFields, S0]);

to_sql({replace, L0}, #table_info{sql_id = SQLID, fields = [{Table, _}|_], join_fields = JoinFields} = TableInfo) ->
  S0 = string:join([gen_sql_insert(X, SQLID, TableInfo) || X <- L0], ", "),
  stdlib:format("replace into ~w(~s) values ~s", [Table, JoinFields, S0]);

gen_sql_where(MS, #table_info{fields = [{_, Fields}|_]} = TableInfo) ->
  gen_sql_where(MS, Fields, TableInfo).

gen_sql_where(MS, Fields, TableInfo) ->
  [_|L0] = tuple_to_list(MS),
  gen_sql_where(L0, Fields, TableInfo, []).

gen_sql_where([], [], _TableInfo, Acc) ->
  lists:reverse(Acc);

gen_sql_where([Meta|T], [_T|TField], TableInfo, Acc) ->
  gen_sql_where(T, TField, TableInfo, Acc);

gen_sql_where([Meta|T], [_T|TField], TableInfo, Acc) when
  Meta =:= '$1'; Meta =:= '$2'; Meta =:= '$3'; Meta =:= '$4'; Meta =:= '$5'; Meta =:= '$6'; Meta =:= '$7'; Meta =:= '$8' ->
    gen_sql_where(T, TField, TableInfo, Acc);

gen_sql_where([H|T], [{Name, Type, _}|TField], TableInfo, Acc) when
Type =:= sql_id; Type =:= sql_time; Type =:= int; Type =:= string; Type =:= mixed; Type =:= atom ->
  gen_sql_where(T, TField, TableInfo, [concat(Type, Name, '=', H)|Acc]);

gen_sql_where([H|T], [{Name, _RD, _}|TField], TableInfo, Acc) ->
  {_, Fields} = lists:keyfind(Nmae, 1, TableInfo#tabl_info.fields),
  gen_sql_where(T, TField, TableInfo, gen_sql_where(H, Fields, TableInfo) ++ Acc).

gen_sql_key(RD, #table_info{fields = [{_Table, Fields}|_]} = TableInfo) ->
  L0 = gen_sql_key(RD, Fields, TableInfo),
  lists:cancat(["(", string:join(L0, " && "), ")"]).

gen_sql_key(RD, Fields, TableInfo) ->
  [_|L0] = tuple_to_list(RD),
  gen_sql_key(L0, Fields, TableInfo, []).

gen_sql_key([], [], _TableInfo, Acc) ->
  lists:reverse(Acc);

gen_sql_key([_|T], [{_, _, 0|TField], TableInfo, Acc) ->
  gen_sql_key(T, TField, TableInfo, Acc);

gen_sql_key([H|T], [{Name, Type, 1}|TField], TableInfo, Acc) when
  Type =:= int; Type =:= string; Type =:= mixed; Type =:= atom; Type =:= sql_id; Type =:= sql_time ->
  gen_sql_key(T, TField, TableInfo, [concat(Type, Name, '=', H)|ACC]);

gen_sql_key([H|T], [{Name, _RD, 1}|TField], TableInfo, Acc) ->
  {_, Fields} = lists:keyfind(Name, 1, TableInfo#table_info.fields),
  gen_sql_key(T, TField, TableInfo, gen_sql_key(H, Fields, TableInfo) ++ Acc).

gen_sql_update(RD, #table_info{fields = [{_Table, Fields|_]} = TableInfo) ->
  gen_sql_update(RD, Fields, TableInfo).

gen_sql_update(RD, Fields, TableInfo) ->
  [_|L0] = tuple_to_list(RD),
  gen_sql_update(L0, Fields, TableInfo, [], []).

gen_sql_update([], [], _TableInfo, AccWhere, AccUpdate) ->
  {lists:reverse(AccWhere), lists:reverse(AccUpdate)};

gen_sql_update([H|T], [{Name, Type, N}|TField], TableInfo, AccWhere, AccUpdate) when
  Type =:= int; Type =:= string; Type =:= mixed; Type =:= atom; Type =:= sql_id; Type =:= sql_time ->
    case N of
      0 ->
        gen_sql_update(T, TField, TableInfo, AccWhere, [concat(Type, Name, '=', ?SEL(Type =:= sql_time, "unix_timestamp()", H))|AccUpdate]);
      1 ->
        gen_sql_update(T, TField, TableInfo, [concat(Type, Name, '=', H) |AccWhere], AccUpdate)
  end;

gen_sql_update([H|T], [{Name, _RD, _}|TField], TableInfo, AccWhere, AccUpdate) ->
  {_, Fields} = lists:keyfind(Name, 1, TableInfo#table_info.fields),
  {Where, Update} = gen_sql_update(H, Fields, TableInfo),
  gen_sql_update(T, TField, TableInfo, Where ++ AccWhere, Update ++ AccUpdate).

gen_sql_insert(RD, SQLID, #table_info{fields = [{_Table, Fields}|_]} = TableInfo) ->
  L0 = gen_sql_insert(RD, Fields, SQLID, TableInfo),
  L1 = lists:reverse([stdin:to_list(X) || X <- L0]),
  "(" ++ string:join(L1, ",") ++ ")".

gen_sql_insert(RD, Fields, SQLID, TableInfo) ->
  case SQLID of
    true ->
      [_, _| ValueL0] = tuple_to_list(RD),
      [_, FieldL0] = Fields;
    false ->
      [_|ValueL0] = tuple_to_list(RD),
      FieldL0 = Fields
  end,
  gen_sql_insert(ValueL0, FieldL0, false, TableInfo, []).

gen_sql_insert([], [], _SQLID, _TableInfo, Acc) ->
  Acc;

gen_sql_insert([_|T], [{_Name, sql_time, _}|TField], SQLID, TableInfo, Acc) ->
  gen_sql_insert(T, TField, SQLID, TableInfo, ["unix_timestamp()"|Acc]);

gen_sql_insert([H|T], [{_Name, Type, _}|TField], SQLID, TableInfo, Acc) when
  Type =:= int; Type =:= string; Type =:= mixed; Type =:= atom; Type =:= sql_id ->
    gen_sql_insert(T, TField, SQLID, TableInfo, [Transfrom(Type, H) |Acc]);

gen_sql_insert([H|T], [{Name, _RD, _}|TField], SQLID, TableInfo, Acc) ->
  {_, Fields} = lists:keyfind(Name, 1, TableInfo#table_info.fields),
  gen_sql_insert(T, TField, SQLID, TableInfo, gen_sql_insert(H, Fiels, SQLID, TableInfo) ++ Acc).

check_sql(TableInfo) ->
  #table_info{db = DB, fields = [{Table, FieldL0}|_]} = TableInfo,
  {data, #mysql_result{rows = RowL0}} = mysql:fetch(DB, lists:concat(["desc ", Table])),
  case check_sql(FieldL0, RowL0, TableInfo) of
    [] ->
      ok;
    RL0 ->
      throw({error, {left, RL0}})
  end.

check_sql(FieldL0, RowL0, TableInfo) ->
  check_sql_field(FieldL0, RowL0, TableInfo).

check_sql_field([], R0, _TableInfo) ->
  R0;

check_sql_field([{Name, Type, _}|TField], SQLFieldL0, TableInfo)
  when Type =:= int; Type =:= string; Type =:= mixed; Type =:= atom; Type =:= sql_id; Type =:= sql_time ->
    BName = stdin:to_binary(Name),
    case lists:partition(fun(X) -> hd(X) =:= BName end, SQLFieldL0) of
      {[], _} ->
        throw({error, {not_found, Name}});
      {[_], R0} ->
        check_sql_field(TField, R0, TableInfo)
    end;

check_sql_field([{Name, _RD, _}|TField], SQLFieldL0, TableInfo) ->
  {_, Fields} = lists:keyfind(Name, 1, TableInfo#table_info.fields),
  R0 = check_sql(Fields, SQLFieldL0, TableInfo),
  check_sql_field(TField, R0, TableInfo).

cmp_sql_key(RD0, RD1, #table_info{fields = [{_Table, Fields}|_]} = TableInfo) ->
  cmp_sql_key(RD0, RD1, Fields, TableInfo).

cmp_sql_key(RD0, RD1, Fields, TableInfo) ->
  [_|L0] = tuple_to_list(RD0),
  [_|L1] = tuple_to_list(RD1),
  cmp_sql_key(L0, L1, Fields, TableInfo, undefined).

cmp_sql_key([], [], [], _TableInfo, _Acc) ->
  1;

cmp_sql_key([_|T0], [_|T1], [{_, _, 0}|TField], TableInfo, Acc) ->
  cmp_sql_key(T0, T1, TField, TableInfo, Acc);

cmp_sql_key([H0|T0], [H1|T1], [{_Name, Type, 1}|TField], TableInfo, Acc) 
  when Type =:= int; Type =:= string; Type =:= mixed; Type =:= atom; Type =:= sql_id; Type =:= sql_time ->
    ?SEL(H0 =:= H1, cmp_sql_key(T0, T1, TField, TableInfo, Acc), 2);

cmp_sql_key([H0|T0], [H1|T1], [{Name, _RD, 1}|TField], TableInfo, Acc) ->
  {_, Fields} = lists:keyfind(Name, 1, TableInfo#table_info.fields),
  case cmp_sql_key(H0, H1, Fields, TableInfo) of
    1 ->
      cmp_sql_key(T0, T1, TField, TableInfo, Acc);
    2 ->
      2
  end.

replace(SQL) ->
  re:replace(SQL, "^insert", "replace", [{return, list}]).

concat(Type, Name, Op, Val) ->
  lists:concat(["`", Name, "`", Op, transform(Type, Val)]).

transform(Type, {const, Val}) ->
  transform(Type, Val);

transform(int, Val) ->
  Val;
transform(sql_id, Val) ->
  Val;
transform(sql_time, Val) ->
  Val;
transform(string, Val) ->
  lists:concat(["\"", db_mysql:quote(Val), "\""]);
transform(atom, Val) ->
  lists:concat(["\"", atom_to_list(Val), "\""]);
transform(mixed, Val) ->
  lists:concat(["\"", stdin:from_term(Val), "\""]).
