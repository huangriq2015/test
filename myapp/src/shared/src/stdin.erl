%% ============================================================================
%% Author            :huangriq - huangriq2015@163.com
%% QQ                :466796514
%% Last modified     :2018-03-13 14:36:11
%% Filename          :*********
%% Description       :************************
%% ============================================================================

-module(stdin).
-include("stdin.hrl").
-include("time.hrl").

-compile(export_all).

round(N) ->
  erlang:round(N).

to_int(S) ->
  if
    S =:= true      -> 1;
    S =:= false     -> 0;
    is_list(S)      -> list_to_integer(S);
    is_bianry(S)    -> list_to_integer(binary_to_list(S));
    is_atom(S)      -> list_to_integer(atom_to_list(S));
    true            -> S
  end.

to_float(S) ->
  try
    list_to_float(S)
  catch
    _:_ ->
      list_to_integer(S)
  end.

to_list(S) ->
  if
    is_binary(S)    -> binary_to_list(S);
    is_bitstring(S) -> bitstring_to_list(S);
    is_atom(S)      -> atom_to_list(S);
    is_integer(S)   -> integer_to_list(S);
    is_float(S)     -> [L0] = io_lib:format("~.2f", [S]), L0;
    is_tuple(S)     -> tuple_to_list(S);
    is_pid(S)       -> pid_to_list(S);
    true ->   S
  end.

to_binary(S) ->
  if 
    is_list(S)      -> list_to_binary(S);
    is_atom(S)      -> atom_to_binary(S, latin1);
    true            -> S
  end.

to_tuple(S) ->
  if
    is_list(S)      -> list_to_tuple(S);
    is_pid(S)       -> list_to_tuple([list_to_integer(X) || X <- string:tokens(pid_to_list(S), "<>.")]);
    true            -> S
  end.

to_atom(S) ->
  if
    is_binary(S)    -> binary_to_atom(S, latin1);
    is_list(S)      -> list_to_atom(S);
    is_integer(S)   -> list_to_atom(integer_to_list(S));
    true            -> S
  end.

to_string(S, Sep) ->
  if
    is_list(S)      -> string:join([to_list(X) || X <- S], Sep);
    is_tuple(S)     -> to_string(to_list(S), Sep);
    true            -> S
  end.

to_strings(L0) ->
  [to_list(X) || X <- L0].

toa(S) ->
  if
    is_tuple(S)     -> inet_parse:ntoa(S);
    is_list(S)  andalso length(S) =:= 4 -> toa(list_to_tuple(S));
    is_integer(S) andalso S < 16#FFFFFFFF -> toa([(S bsr X) band 16#FF || X <- [24, 16, 8, 0]]);
    true            -> S
  end.

tou(S) ->
  if
    is_list(S) andalso length(S) =:= 4 -> list_to_tuple(S);
    is_list(S)    -> {ok, T1} = inet_parse:address(S), T1;
    is_integer(S) andalso S < 16#FFFFFFFF -> list_to_tuple([(S bsr X) band 16#FF || X <- [24, 16, 8, 0]]);
    true -> S
  end.

floor(S) ->
  R = trunc(S),
  ?SEL(R > S, R - 1, R).

ceil(S) ->
  R = trunc(S),
  ?SEL(R < S, R + 1, R).

nearby_element([H|T], Ref) ->
  nearby_element_next(T, Ref, H).

nearby_element_next([], _Ref, Floor) ->
  Floor;
nearby_element_next([H|T], Ref, _Floor) when H =< Ref ->
  nearby_element_next(T, Ref, H);
nearby_element_next(_L0, _Ref, Floor) ->
  Floor.

nearby_element([H|T], Index, Ref) ->
  nearby_element_next(T, Index, Ref, H).

nearby_element_next([], _Index, _Ref, Floor) ->
  Floor;
nearby_element_next([H|T], Index, Ref, Floor) ->
  N = erlang:element(Index, H),
  case N =< Ref of
    true ->
      nearby_element_next(T, Index, Ref, H);
    false ->
      Floor
  end.

send(Tag, Msg) ->
  case whereis(Tag) of
    undefined ->
      ?ERROR("PROCESS NOT EXISTS:~w,~w", [Tag, Msg]);
    PID ->
      PID ! Msg
  end,
  ignore.

call(Tag, Msg) ->
  call(Tag, Msg, 1000).

call(Tag, Msg, Timeout) ->
  case catch gen_server:call(whereis(Tag), Msg, Timeout) of
    {'EXIT', {timeout, _} = Result} ->
      ?ERROR("调用超时:~w,~w,~w", [Tag, Msg, Result]),
      Result;
    {'EXIT', Result} ->
      Result;
    {throw, Result} ->
      Result;
    Result        ->
      Result
  end.

whereis(Tag) when is_atom(Tag) ->
  erlang:whereis(Tag);
whereis(Tag) when is_pid(Tag) ->
  Tag;
whereis(Tag) when is_port(Tag) ->
  Tag;
whereis(Tag) ->
  globalmgr:whereis_name(Tag).

delete_child(Sup, Child) ->
  supervisor:terminate_child(Sup, Child),
  supervisor:delete_child(Sup, Child).

stop(Reason) ->
  stop(self(), Reason).

stop(PID, Reason) ->
  PID ! {'EXIT', Reason}.

time() ->
  {MegaS, S, _} = now(),
  MegaS * 1000000 + S.

mtime() ->
  {MegaS, S, US} = now(),
  MegaS * 1000000000 + S * 1000 + US div 1000.

now() ->
  os:timestamp().

mktime(DT) ->
  calendar:datetime_to_gregorian_seconds(erlang:localtime_to_universaltime(DT)) - ?SECONDS_0_1970.

localtime() ->
  localtime(time()).

localtime(S) ->
  erlang:universaltime_to_localtime(transform(S)).

week() ->
  {YMD, _} = localtime(),
  week(YMD).

week(S) when is_integer(S) ->
  {YMD, _} = localtime(S),
  week(YMD);
week({Y, M, D}) ->
  calendar:iso_week_number({Y, M, D}).

is_sameyear(S1, S2) ->
  {{Y1, _, _}, {_, _, _}} = localtime(S1),
  {{Y2, _, _}, {_, _, _}} = localtime(S2),
  Y1 =:= Y2.

is_samemonth(S1, S2) ->
  {{Y1, M1, _}, {_, _, _}} = localtime(S1),
  {{Y2, M2, _}, {_, _, _}} = localtime(S2),
  Y1 =:= Y2 andalso M1 =:= M2.

is_sameday(S1, S2) ->
  {{Y1, M1, D1}, {_, _, _}} = localtime(S1),
  {{Y2, M2, D2}, {_, _, _}} = localtime(S2),
  Y1 =:= Y2 andalso M1 =:= M2 andalso D1 =:= D2.

is_sameweek(S1, S2) ->
  week(S1) =:= week(S2).

transform(S) ->
  calendar:gregorian_seconds_to_datetime(S + ?SECONDS_0_1970).

cmp([], _) ->
  true;
cmp(_, []) ->
  false;
cmp([H0|_T0], [H1|_T1]) when H0 < H1 ->
  true;
cmp([H0|_T0], [H1|_T1]) when H0 > H1 ->
  false;
cmp([_|T0], [_|T1]) ->
  cmp(T0, T1).


ge([], _) ->
  true;
ge(_, []) ->
  false;
ge([H0|_T0], [H1|_T1]) when H0 > H1 ->
  true;
ge([H0|_T0], [H1|_T1]) when H0 < H1 ->
  false;
ge([_|T0], [_|T1]) ->
  ge(T0, T1).

cancel_timer(Ref) when is_reference(Ref) ->
  erlang:cancel_timer(Ref);
cancel_timer(_) ->
  false.

sleep(Ms) ->
  timer:sleep(Ms).


