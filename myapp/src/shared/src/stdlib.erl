%% ============================================================================
%% Author            :huangriq - huangriq2015@163.com
%% QQ                :466796514
%% Last modified     :2018-03-13 09:38:37
%% Filename          :*********
%% Description       :************************
%% ============================================================================

-module(stdlib).
-export([
    %% 最小堆轮询
    mh_set/2,
    mh_set/3,
    mh_empty/0,
    mh_add/1,
    mh_del/1,
    mh_chk/1,
    mh_find/1,
    mh_loop/2
  ]).

-include("stdin.hrl").

-spec mh_set(FunLoop :: minheap:cmp_fun(), FunAdd :: minheap:cmp_fun()) -> no_return().
mh_set(FunLoop, FunAdd) ->
    set_mh_loop(FunLoop),
    set_mh(minheap:new(FunAdd)).

-spec mh_set(FunLoop :: minheap:cmp_fun(), FunAdd :: minheap:cmp_fun(), Args :: list()) -> no_return().
mh_set(FunLoop, FunAdd, Args) ->
  set_mh_loop(FunLoop),
  set_mh(minheap:new(FunAdd, Args)).

-spec mh_empty() -> ok.
mh_empty() ->
  Mh = get_mh(),
  set_mh(minheap:empty(Mh)),
  ok.

-spec mh_add(Elem :: tuple()) -> ok.
mh_add(Elem) ->
  Mh = get_mh(),
  set_mh(minheap:push(Elem, Mh)),
  ok.

-spec mh_del(ID :: term()) -> ok.
mh_del(ID) ->
  Mh = get_mh(),
  set_mh(minheap:remove(ID, Mh)),
  ok.

-spec mh_loop(Index :: integer(), Ref :: term()) -> ok.
mh_loop(Index, Ref) ->
  Mh = get_mh(),
  {Mod, CMP} = get_mh_loop(),
  case minheap:size(Mh) > 0 of
    true ->
      Elem = minheap:top(Mh),
      case Mod:CMP(element(Index, Elem), Ref) of
        true ->
          %% 删除事件
          set_mh(minheap:pop(Mh)),
          ?CATCH(Mod:handle_mh(Elem, Ref)),
          mh_loop(Index, Ref);
        false ->
          ignore
      end;
    false ->
      ignore
  end.

-spec mh_chk(ID :: term()) -> boolean().
mh_chk(ID) ->
  mh_find(ID) =/= error.

-spec mh_find(ID :: term()) -> error | {ok, term()}.
mh_find(ID) ->
  Mh = get_mh(),
  minheap:find(ID, Mh).

set_mh_loop(Fun) ->
  put('__mh_loop__', Fun).

get_mh_loop() ->
  get('__mh_loop__').

set_mh(Mh) ->
  put('__mh__', Mh).

get_mh() ->
  get('__mh__').
