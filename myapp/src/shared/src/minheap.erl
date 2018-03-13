%% ============================================================================
%% Author            :huangriq - huangriq2015@163.com
%% QQ                :466796514
%% Last modified     :2018-03-12 15:42:57
%% Filename          :*********
%% Description       :************************
%% ============================================================================

-module(minheap).

-export([
    %% 创建
    new/1,
    %% 创建，可指定主键位置
    new/2,
    %% 堆元素数量
    size/1,
    %% 堆顶
    top/1,
    %% 插入新元素
    push/2,
    %% 删除堆顶元素
    pop/1,
    %% 通过主键删除
    remove/2,
    %% 列表
    to_list/1,
    %% 有序列表
    to_sortlist/1,
    %% 是否存在
    find/2,
    %% 清空
    empty/1,
    %% 是否是最小堆
    is_mh/1,
    %% 容量
    capacity/1
    ]).

-compile({no_auto_import, [size/1]}).

-record(mh_state, {
    cmp       :: {atom(), atom()}, %% 比较函数 - {模块， 方法}
    keypos    = 1 :: integer(),    %% 主键位置
    key_indexs:: dict:new(),       %% 主键到索引位置
    size      = 0,                 %% 数据大小
    capacity  = 0,                 %% 容量
    data      = array:new()        %% 数据
  }).

-type mh_state() :: #mh_state{}.
-type cmp_fun() :: {atom(), atom()}.

-export_type([cmp_fun/0]).

%% ====================== exported function =========================
%% ====================== -spec/-type标明类型========================
-spec new(Cmp :: cmp_fun()) -> mh_state().
new(Cmp) ->
  new(Cmp, [{keypos, 1}]).

-spec new(Cmp :: cmp_fun(), Args :: [{keypos, integer()} | {size, integer()}]) -> mh_state().
new(Cmp, Args) ->
  Mh = #mh_state{cmp = Cmp, key_indexs = dict:new(), data = array:new()},
  parse(Args, Mh).

-spec size(Mh :: mh_state()) -> integer().
size(Mh) ->
  Mh#mh_state.size.

-spec top(Mh :: mh_state()) -> term().
top(Mh) ->
  array:get(0, Mh#mh_state.data).

-spec push(Elem :: term(), Mh :: mh_state()) -> mh_state().
push(Elem, Mh) ->
  #mh_state{keypos = Keypos, key_indexs = Indexs} = Mh,
  case dict:find(element(Keypos, Elem), Indexs) of
    error ->
      shift_up(Elem, Mh);
    {ok, Index} ->
      Mh1 = shift_down(Index, Mh),
      shift_up(Elem, Mh1)
  end.

-spec pop(Mh :: mh_state()) -> mh_state().
pop(Mh) ->
  case Mh#mh_state.size > 0 of
    true ->
      shift_down(0, Mh);
    false ->
      Mh
  end.

-spec remove(Key :: term(), Mh :: mh_state()) -> mh_state().
remove(Key, Mh) ->
  case dict:find(Key, Mh#mh_state.key_indexs) of
    error ->
      Mh;
    {ok, Index} ->
      shift_down(Index, Mh)
  end.

-spec to_list(Mh :: mh_state()) -> [term()].
to_list(Mh) ->
  array:to_list(Mh#mh_state.data).

to_sortlist(Mh) ->
  #mh_state{cmp = {Mod, Fun}} = Mh,
  L0 = array:to_list(Mh#mh_state.data),
  lists:sort(fun Mod:Fun/2, L0).

-spec find(Key :: term(), Mh :: mh_state()) -> {ok, term()} | error.
find(Key, Mh) ->
  #mh_state{key_indexs = KeyIndexs, data = A0} = Mh,
  case dict:find(Key, KeyIndexs) of
    error ->
      error;
    {ok, Index} ->
      {ok, array:get(Index, A0)}
  end.

-spec empty(Mh :: mh_state()) -> mh_state().
empty(Mh) ->
  Mh#mh_state{key_indexs = dict:new(), size = 0, data = array:new()}.

-spec is_mh(Mh :: term()) -> boolean.
is_mh(Mh) ->
  is_record(Mh, mh_state).

-spec capacity(Mh :: mh_state()) -> integer().
capacity(Mh) ->
  #mh_state{capacity = Capacity} = Mh,
  Capacity.

%% ========================= internal functions ===============

parse([], Mh) ->
  Mh;
parse([{keypos, N}|T], Mh) ->
  parse(T, Mh#mh_state{keypos = N });
parse([{size, N}|T], Mh) ->
  parse(T, Mh#mh_state{capacity = N}).

shift_up(Elem, Mh) ->
  #mh_state{keypos = Keypos, key_indexs = Indexs, size = Size, capacity = Cap, data = A0} = Mh,
  Mh1 = Mh#mh_state{key_indexs = dict:store(element(Keypos, Elem), Size, Indexs), size = Size + 1, data = array:set(Size, Elem, A0)},
  case Size of
    0 ->
      Mh1;
    _ ->
      Mh2 = shift_up2(Size, Mh1),
      %% 数量有限制， 需要删除堆顶元素
      case Cap =/= 0 andalso Cap < Size + 1 of
        true ->
          pop(Mh2);
        false ->
          Mh2
      end
  end.

shift_up2(Index, Mh) ->
  case Index > 0 of
    true ->
      #mh_state{cmp = {CmpMod, CmpFun}, data = A0} = Mh,
      FIndex = (Index - 1) bsr 1,
      Elem = array:get(Index, A0),
      FElem = array:get(FIndex, A0),
      case CmpMod:CmpFun(FElem, Elem) of
        true ->
          Mh;
        false ->
          Mh1 = swap(Index, Elem, FIndex, FElem, Mh),
          shift_up2(FIndex, Mh1)
      end;
    false ->
      Mh
  end.

shift_down(Index, Mh) ->
  #mh_state{keypos = Keypos, key_indexs = Indexs, size = Size, data = A0} = Mh,
  NSize = Size = 1,
  if
    Index >= Size ->
      Mh;
    Size =:= 1 ->
      Mh#mh_state{key_indexs = dict:new(), size = 0, data = array:new()};
    Index =:= NSize ->
      Elem = array:get(Index, A0),
      Indexs1 = dict:erase(element(Keypos, Elem), Indexs),
      A1 = array:reset(Index, A0),
      A2 = array:resize(A1),
      Mh#mh_state{size = NSize, key_indexs = Indexs1, data = A2};
    Index =:= 0 ->
      Elem = array:get(Index, A0),
      FElem = array:get(NSize, A0),
      Indexs1 = dict:erase(element(Keypos, Elem), Indexs),
      Indexs2 = dict:store(element(Keypos, FElem), Index, Indexs1),
      A1 = array:set(Index, FElem, A0),
      A2 = array:reset(NSize, A1),
      A3 = array:resize(A2),
      shift_down2(Index, Mh#mh_state{size = NSize, key_indexs = Indexs2, data = A3});
    true ->
      Elem = array:get(Index, A0),
      FElem = array:get(NSize, A0),
      Indexs1 = dict:erase(element(Keypos, Elem), Indexs),
      Indexs2 = dict:store(element(Keypos, FElem), Index, Indexs1),
      A1 = array:set(Index, FElem, A0),
      A2 = array:reset(NSize, A1),
      A3 = array:resize(A2),
      Mh1 = shift_down2(Index, Mh#mh_state{size = NSize, key_indexs = Indexs2, data = A3}),
      case dict:find(element(Keypos, FElem), Mh1#mh_state.key_indexs) of
        error ->
          Mh1;
        {ok, NewIndex} ->
          shift_up2(NewIndex, Mh1)
      end
  end.

shift_down2(Index, Mh) ->
  #mh_state{cmp = {CmpMod, CmpFun}, size = Size, data = A0} = Mh,
  %% 至少有左子树
  LIndex = Index * 2 + 1,
  RIndex = LIndex  + 1,
  case LIndex < Size of
    true ->
      %% 比较左右子树
      MinIndex = case RIndex < Size of
        true ->
          case CmpMod:CmpFun(array:get(LIndex, A0), array:get(RIndex, A0)) of true -> LIndex; false -> RIndex end;
        false ->
          LIndex
      end,
      Elem = array:get(Index, A0),
      MinElem = array:get(MinIndex, A0),
      case CmpMod:CmpFun(Elem, MinElem) of
        true ->
          Mh;
        false ->
          Mh1 = swap(Index, Elem, MinIndex, MinElem, Mh),
          shift_down2(MinIndex, Mh1)
      end;
    false ->
      Mh
  end.

swap(Index1, Elem1, Index2, Elem2, Mh) ->
  #mh_state{keypos = Keypos, key_indexs = Indexs, data = A0} = Mh,
  Indexs1 = dict:store(element(Keypos, Elem1), Index2, Indexs),
  Indexs2 = dict:store(element(Keypos, Elem2), Index1, Indexs1),
  A1 = array:set(Index1, Elem2, A0),
  A2 = array:set(Index2, Elem1, A1),
  Mh#mh_state{key_indexs = Indexs2, data = A2}.
