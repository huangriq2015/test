-module(db).
-export([
    read/2,
    match/2,
    select/2,
    write/2,
    write/3,
    replace/2,
    delete/2,
    delete_object/2,
    delete_match_object/2,
    insert_id/1,
    pdate_insert_id/2,
    clear/1,
    transaction/1,
    abort/1
    ]).

-include("stdin.hrl").
-include("db.hrl").

read(Tab, K) ->
  InTransaction = in_transaction(),
  read(Tab, K, check_log(Tab, InTransaction), InTransaction).

read(Tab, K, true, InTransaction) ->
  read_process(Tab, K, InTransaction);

read(Tab, K, false, InTransaction) ->
  case read_cache(Tab, K, InTransaction) of
    [] ->
      case check_load(Tab, InTransaction) orelse check_match(Tab, K, InTransaction) of
        true ->
          [];
        false ->
          case read_process(Tab, K, InTransaction) of
            [] ->
              write_cache(t_builtin, #t_builtin{key = {match, Tab, K}, value = true}, InTransaction),
              [];
            L0 when is_list(L0) ->
              dbmgr:set_data_timeout(Tab, K),
              write_cache(Tab, L0, InTransaction),
              L0
          end
      end;
    L0 ->
      L0
  end.

match(Tab, MS) ->
  match(Tab, MS, match, in_transaction()).

select(Tab, MS) ->
  match(Tab, MS, select in_transaction()).

match(Tab, MS, Mode, InTransaction) ->
  case check_load(Tab, InTransaction) of
    true ->
      match_cache(Tab, MS, Mode, InTransaction);
    false ->
      match_process(Tab, MS, Mode, InTransaction)
  end.

write(Tab, V) ->
  write2(Tab, V, true).

write(Tab, K, V) when is_tuple(V) ->
  write(Tab, K, [V]);

write(Tab, K, V) ->
  InTransaction = in_transaction(),
  write2(Tab, K, V, InTransaction, true).

replace(Tab, V) ->
  write2(Tab, V, false).

write2(_Tab, [], false) ->
  ok;

write(_Tab, [], true) ->
  ok;

write2(_Tab, undefined, _Replace) ->
  ok;

write2(Tab, V, Replace) when is_tuple(V) ->
  write2(Tab, [V], Replace);

write2(Tab, V, Replace) ->
  InTransaction = in_transaction(),
  case check_log(Tab, InTransaction) of
    true ->
      write_log_process(Tab, V);
    false ->
      [write2(Tab, XK, lists:reverse(XV), InTransaction, Replace) || {XK, XV} <- split(V)]
  end,
  ok.

write2(Tab, K, V, InTransaction, Replace) ->
  L0 = read(Tab, K, false, InTransaction),
  {Chg, DelL0, OldL0, RepL0, AddL0} = partition(Tab, Replace, V, L0),
  %% 判断数据是否修改
  case Chg of
    true ->
      not check_load(Tab, InTransaction) andalso dbmgr:set_data_timeout(Tab, K),
      RepL0 =/= [] andalso update_object(Tab, OldL0, RepL0, InTransaction),
      DelL0 =/= [] andalso delete_object(Tab, DelL0, InTransaction),
      AddL0 =/= [] andalso write_object(Tab, AddL0, InTransaction);
    false ->
      ignore
  end,
  ok.

update_object(Tab, OldL0, NewL0, InTransaction) ->
  case mnesia:table_info(Tab, type) of
    set ->
      ignore;
    bag ->
      delete_object_cache(Tab, OldL0, InTransaction)
  end,
  update_process(Tab, NewL0, InTransaction),
  update_cache(Tab, NewL0, InTransaction).

write_object(Tab, L0, InTransaction) ->
  write_process(Tab, L0, InTransaction),
  write_cache(Tab, L0, InTransaction).

delete(Tab, K) ->
  InTransaction = in_transaction(),
  delete_process(Tab, K, InTransaction),
  delete_cache(Tab, K, InTransaction).

delete_object(Tab, V0) ->
  InTransaction = in_transaction(),
  delete_object(Tab, V, InTransaction).

delete_object(Tab, V, InTransaction) ->
  delete_object_process(Tab, V, InTransaction),
  delete_object_cache(Tab, V, InTransaction）。

delete_match_object(Tab, MS) when is_tuple(MS) ->
  InTransaction = in_transaction(),
  delete_match_object_process(Tar, MS, InTransaction),
  delete_match_object_cache(Tab, MS, InTransaction).

insert_id(Tab) ->
  InTransaction = in_transaction(),
  case read_cache(t_builtin, {auto_increment, Tab}, InTransaction) of
    [#t_builtin{value = ID}] ->
      ignore;
    _       ->
      ID = insert_id_process(Tab, InTransaction)
  end,
  write_cache(t_builtin, #t_builtin{key = {auto_increment, Tab}, value = ID + 1}, InTransaction),
  ID.

update_insert_id(Tab, ID) ->
  InTransaction = in_transaction(),
  write_cache(t_builtin, #t_builtin{key = {auto_increment, Tab}, value = ID}, InTransaction).

clear(Tab) ->
  clear_process(Tab),
  clear_cache(Tab).

transaction(Fun) ->
  put('__transaction__', true),
  case mnesia:transaction(fun() -> set_sql([]), R = Fun(), transaction_sql(), R end) of
    {atomic, _} = Res   ->
      ignore;
    {aborted, {throw, Err}} ->
      Res = {aborted, Err};
    Res ->
      ignore
  end,
  erase('__transaction__'),
  Res.

transaction_sql() ->
  L0 = lists:reverse(del_sql()),
  RoleID = role_data:catch_get_roleid(),
  {atomic, _} = db_table:transaction(fun() -> db_table:set_roleid(RoleID), [execute_process(X) || X <- L0] end).

abort(Reason) ->
  mnesia:abort(Reason).


check_log(Tab, InTransaction) ->
  case read_cache(t_builtin, {log, Tab}, InTransaction, read) of
    [#t_builtin{value = true}] ->
      true;
    _ ->
      false
  end.

check_load(Tab, InTransaction) ->
  case read_cache(t_builtin, {load, Tab}, InTransaction, read) of
    [#t_builtin{value = true}] ->
      true;
    _ ->
      false
  end.

check_match(Tab, K, InTransaction) ->
  case read_cache(t_builtin, {match, Tab, K}, InTransaction) of
    [#t_builtin{value = true}] ->
      true;
    _ ->  
      false
  end.

in_transaction() ->
  get('__transaction__') =:= true.

read_cache(Tab, K, true) ->
  mnesia:read(Tab, K, write);

read_cache(Tab, K, true, LockKind) ->
  mnesia:read(Tab, K, LockKind);

read_cache(Tab, K, false, _LockKind) ->
  mnesia:dirty_read(Tab, K).

write_cache(Tab, V, InTransaction) when is_list(V) ->
  [write_cache(Tab, X, InTransaction) || X <- V];

write_cache(Tab, V, true) ->
  mnesia:write(Tab, V, write);
  
write_cache(Tab, V, false) ->
  mnesia:dirty_write(Tab, V).

update_cache(Tab, V, InTransaction) ->
  write_cache(Tab, V, InTransaction).

delete_cache(Tab, K, true) ->
  mnesia:delete(Tab, K, write);

delete_cache(Tab, K, false) ->
  mnesia:dirty_delete(Tab, K).

delete_object_cache(Tab, V, InTransaction) when is_list(V) ->
  [delete_object_cache(Tab, X, InTransaction) || X <- V];

delete_object_cache(Tab, V, true) ->
  mnesia:delete_object(Tab, V, write);

delete_object_cache(Tab, V, false) ->
  mnesia:dirty_delete_object(Tab, V).

delete_match_object_cache(Tab, MS, InTransaction) ->
  L0 = match_cache(Tab, MS, match, InTransaction),
  delete_object_cache(Tab, L0, InTransaction).

match_cache(Tab, MS, select, true) ->
  mnesia:select(Tab, MS, write);

match_cache(Tab, MS, select, false) ->
  mnesia:dirty_select(Tab, MS);

match_cache(Tab, MS, match, true) ->
  mnesia:match_object(Tab, MS, write);

match_cache(Tab, MS, match, false) ->
  mnesia:dirty_match_object(Tab, MS).

clear_cache(Tab) ->
  mnesia:clear_table(Tab).

insert_id_process(Tab, InTransaction) ->
  InTransaction andalso mnesia:lock({table, Tab}, write),
  db_table:insert_id(Tab).

read_process(Tab, K, InTransaction) ->
  InTransaction andalso mnesia:lock({table, Tab}, read),
  db_table:read(Tab, K).

write_process(Tab, V, true) ->
  add_sql({insert, Tab, V});
write_process(Tab, V, false) ->
  db_table:write(Tab, V).

update_process(Tab, V, true) ->
  add_sql({update, Tab, V});

update_process(Tab, V, false) ->
  [db_table:update(Tab, X) || X <- V].

delete_process(Tab, K, true) ->
  add_sql({delete, Tab, K});

delete_process(Tab, V, false) ->
  db_table:delete(Tab, V).

delete_object_process(_Tab, [], _InTransaction) ->
  ok;

delete_object_process(Tab, V, true) ->
  add_sql({delete_object, Tab, V});

delete_object_process(Tab, V, false) ->
  db_table:delete_object(Tab, V).

delete_match_object_process(Tab, V, true) ->
  add_sql({delete_match_object, Tab, V});

delete_match_object_process(Tab, V, false) ->
  db_table:delete_match_object(Tab, V).

match_process(Tab, MS, Mode, InTransaction) ->
  InTransaction andalso mnesia:lock({table, Tab}, write),
  db_table:match(Tab, {Mode, MS}).

clear_process(Tab) ->
  db_table:clear(Tab).

write_log_process(Tab, V) ->
  Cnt = length(V),
  case mnesia:dirty_read(t_builtin, {pools, Tab}) of
    [#t_builtin{value = {[{PID, N}|T], Used}}] ->
      Tol = N + Cnt,
      mnesia:dirty_write(t_builtin, #t_builtin{key = {pools, Tab}, value = ?SEL(Tol >= ?DATACNT, {T, [{PID, 0}|Used]}, {[{PID, Tol}|T], Used})}),
      stdin:send(PID, {write, V});
    [#t_builtin{value = {[], [_|_] = Used}}] ->
      [{PID, _}|T] = lists:reverse(Used),
      mnesia:dirty_write(t_builtin, #t_builtin{key = {pools, Tab}, value = {[{PID, Cnt}|T], []}}),
      stdin:send(PID, {write, V});
    Err ->
      ?ERROR("没有可用连接池:~w,~w", [Tab, Err])
  end.

execute_process({insert, Tab, V}) ->
  write_process(Tab, V, false);

execute_process({update, Tab, V}) ->
  update_process(Tab, V, false);

execute_process({delete, Tab, V}) ->
  delete_process(Tab, V, false);

execute_process({delete_object, Tab, V}) ->
  delete_object_process(Tab, V, false);

execute_process({delete_match_object, Tab, V}) ->
  delete_match_object_process(Tab, V, false).

set_sql(V) ->
  put('__sql__', V).

get_sql() ->
  get('__sql__').

add_sql(V) ->
  L0 = get_sql(),
  set_sql([V|L0]).

del_sql() ->
  erase('__sql__').

split(L0) ->
  split(L0, []).

split([], Acc) ->
  Acc;
split([H|T], Acc) ->
  K = element(2, H),
  case lists:keytake(K, 1, Acc) of
    false ->
      split(T, [{K, [H]} | Acc]);
    {value, {K, L0}, R0} ->
      split(T, [{K, [H|L0]}|R0])
  end.

partition(Tab, Replace, NewL0, OldL0) ->
  partition(NewL0, OldL0, Tab, Replace, false, [], [], [], []).

partition([], [], _Tab, _Replace, AccChg, AccDelL0, AccOldL0, AccRepL0, AccAddL0) ->
  {AccChg, AccDelL0, AccOldL0, AccRepL0, AccAddL0};

partition([], OldL0, _Tab, true, _AccChg, AccDelL0, AccOldL0, AccRepL0, AccAddL0) ->
  {true, OldL0 ++ AccDelL0, AccOldL0, AccRepL0, AccAddL0};

partition([], _OldL0, _Tab, false, AccChg, AccDelL0, AccOldL0, AccRepL0, AccAddL0) ->
  {AccChg, AccDelL0, AccOldL0, AccRepL0, AccAddL0};

partition(NewL0, [], _Tab, _Replace, _AccChg, AccDelL0, AccOldL0, AccRepL0, AccAddL0) ->
  {true, AccDelL0, AddOldL0, AccRepL0, NewL0 ++ AccAddL0};

partition([NH|NT], OldL0, Tab, Replace, AccChg, AccDelL0, AccOldL0, AccRepL0, AccAddL0) ->
  case find(Tab, NH, OldL0) of
    {0, _, ROldL0} ->
      partition(NT, ROldL0, Tab, Replace, AccChg, AccDelL0, AccOldL0, AccRepL0, AccAddL0);
    {1, E, ROldL0} ->
      partition(NT, ROldL0, Tab, Replace, true, AccDelL0, [E|AccOldL0], [NH|AccRepL0], AccAddL0);
    {2, _, ROldL0} ->
      partition(NT, ROldL0, Tab, Replace, true, AccDelL0, AccDelL0, AccRepL0, [NH|AccAddL0])
  end.

%% 0 相同 1 相同主键 2不同
find(Tab, E, L0) ->
  [#t_builtin{value = TableInfo}] = ets:lookup(t_builtin, {record, Tab}),
  find(L0, E, TableInfo, []).

find([], E, _TableInfo, Acc) ->
  {2, E, Acc};
find([H|T], E, TableInfo, Acc) ->
  case db_table:cmp(E, H, TableInfo) of
    0 -> 
      {0, H, T ++ Acc};
    1 ->
      {1, H, T ++ Acc};
    2 ->
      find(T, E, TableInfo, [H|Acc])
  end.

