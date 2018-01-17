%% ============================================================================
%% Author            :huangriq - huangriq2015@163.com
%% QQ                :466796514
%% Last modified     :2017-06-27 14:19:58
%% Filename          :*********
%% Description       :************************
%% ============================================================================
-module(gate).

-export([
    %% 创建SOCKET
    open/2,
    %% 关闭SOCKET
    close/2,
    %% 接收到TCP数据
    recv/2,
    %% 进程其他数据
    message/1,
    %% 进程其他数据
    message_return/1,
    %% 停止
    stop/1,
    

    %% 定时器
    mh_add_intvl/3,
    mh_add_ends/3,
    mh_del/1,
    cmp_add/2
    cmp_loop/2,
    handle_mh/2,
    loop/1,

    %% 最小堆回调
    check_packet/1,
    check_packet_exceed/1,
    check_timeout/1
    ]).

-include("gate.hrl").


-spec open(Sock :: port(), UserData :: term()) -> no_return().
open(Sock, _UserData) ->
  stdlib:process_tag(?MODULE),
  %% SOCK信息
  {ok, {LocalIP, LocalPort}} = inet:sockname(Sock),
  {ok, {PeerIP, PeerPort}}   = inet:peername(Sock),
gate_data:set_sockaddr(#b_sockaddr{sock = Sock, local_ip = LocalIP, local_port = LocalPort,
              peer_ip = PeerIP, peer_port = PeerPort}),
  gate_data:set_seq(0),
  gate_data:set_buff(<<>>),
  gate_data:set_heartbeat(0),
  gate_data:set_timeout_count(0),
  gate_data:set_packet_count(dict:new()),
  gate_data:set_packet_unexpected([]),

  %% 启动定时器
  stdlib:mh_set({?MODULE, cmp_loop}, {?MODULE, cmp_add}),
  mh_add_intvl(check_packet, f_etc:find(check_packet_intvl), undefined),
  mh_add_intvl(check_packet_exceed, f_etc:find(check_packet_period), undefined),
  mh_add_intvl(check_timeout, f_etc:find(heartbeat_timeout), undefined),

  timermgr:send_interval(?LOOP, self(), ?MODULE, loop),
  
  ?WARN("SOCKET CREATE:~w, ~w:~w => ~w:~w", [Sock, LocalIP, LocalPort, PeerIP, PeerPort]).


-spec close(Sock :: port(), Reason :: term()) -> no_return().
close(Sock, Reason) ->
  ?WARN("SOCKET CLOSE :~w,~w", [Sock, Reason])

-spec recv(Sock :: prot(), Msg :: binary()) -> no_return().
recv(_Sock, Msg) ->
  handle(<<(gate_data:get_buff())/binary, Msg/binary>>).

message(Msg) ->
  handle_msg(Msg).

message_return(Msg) ->
  handle_msg(Msg).

stop(Reason) ->
  case gate_data:get_rolepid() of
    undefined ->
      ignore;
    PID ->
      stdin:send(PID, {tcp_closed, self(), Reason})
  end,
  case is_integer(Reason) orelse (is_tuple(Reason) andalso is_integer(element(1, Reason))) of
    true ->
      stdlib:send_error(#m_system_quit_toc{}, Reason),
      RoleID = gate_data:get_roleid(),
      globalmgr:unregister_name(stdlib:gate_procname(RoleID));
    false ->
      ignore
  end,
  case gate_data:get_roleref() of
    undefined ->
      ignore;
    Ref ->
      erlang:demonitor(Ref, [flush])
  end,
  stdin:sleep(800),
  stdin:stop(Reason).

shutdown(Reason) ->
  gate_data:set_rolepid(undefined),
  gate_data:set_roleref(undefined),
  stop(Reason).


handle(Msg0) ->
  case pt:read({binary, Msg0}) of
    {Msg1, Msg2} ->
      {Hdr, _} = pt:read({m_hdr, Msg1}),
      #m_hdr{module = Module, method = Method} = Hdr,

      case catch parse(Hdr, Msg1) of
        {ok, RD} ->
          gate_data:set_packet_time(stdin:time()),
          add_packet_count(Module, Method),
          message(RD),
          handle(Msg2);
        {error, Err} ->
          debugmgr:print_msg(Hdr, "[~w]RECV:~w", [gate_data:get_roleid(), Hdr),
          case lists:member(Err, "FATAL_ERR") of
            true ->
              ?ERROR("解析数据错误：~w,~w,~w", [Err, Hdr, Msg1]);
            false ->
              ignore
          end,
          case lists:member(Err, [?KEERNO_SYSTEM_FORMAT]) of
            true ->
              ?ERROR("解析数据出错，断开socket:~w,~w,~w", [Err, Hdr, Msg1]),
              stop(?KERRNO_VERSION);
            false ->
              gate_data:set_buff(Msg2),
              stdlib:send_error(Module, Method, Err)
          end;
        _Err        ->
          debugmgr:print_msg(Hdr, "[~w]RECV:~w", [gate_data:get_roleid(), Hdr]),
          ?ERROR("解析数据错误:~w,~w,~w", [_Err, Hdr, Msg1]),
          get_data:set_buff(Msg2),
          stdlib:send_error(Module, Method, ?KERRNO_SYSTEM)
      end;
    _ ->
      gate_data:set_buff(Msg0)
  end.

perse(#m_hdr{seq = Seq, module = Module, method = Method}, Msg) ->
  %% 检查序列号
  case check_seq(Seq) of
    false ->
      throw({error, ?KERRNO_SYSTEM_SEQENCE});
    true ->
      ignore
  end,
  %% 检查检验和
  case check_checksum(Msg, 0) of
    false ->
      throw({error, ?KERRNO_SYSTEM_CHECKSUM});
    true ->
      ignore
  end,
  %% 协议定义
  case pt_conf:find(Module, Method) of
    undefined ->
      RDName = undefined,
      throw({error, ?KEERNO_SYSTEM_MM_UNDEFINED});
    RDName ->
      ignore
  end,
  %% 协议禁用
  case f_mm_func:find({Module, 0}) of
    #f_mm_func{enable = false} ->
      throw({error, ?KERRNO_SYSTEM_MM_DISABLED});
    _ ->
      ignore
  end,
  case f_mm_func:find({Module, Method}) of
    #f_mm_func{enable = false} ->
      throw({error, ?KERRNO_SYSTEM_MM_DISABLED});
    _ ->
      ignore
  end,
  %% 解析协议
  case pt:read({RDName, Msg}) of
    {RD, <<>>} ->
      ignore;
    _ ->
      RD = undefined,
      throw({error, ?KERRNO_SYSTEM_FORMAT})
  end,

  debugmgr:print_msg(RD, "[~w]RECV:~w", [gate_data:get_roleid(), RD]),

  gate_chk:check(RD),

  {ok, RD}.


check_seq(Seq) ->
  case gate_data:get_seq(),
    Seq ->
      gate_data:set_seq((Seq + 1) rem ?MAX_SEQ),
      true;
    _ ->
      false
  end.

check_checksum(<<>>, Sum) when Sum > 16#FFFF ->
  check_checksum(<<>>, (Sum bsr 16) + (Sum band 16#FFFF));
check_checksum(<<>>, Sum) ->
  Sum =:= 16#ffff;
check_checksum(<<N:8/little-unsigned-integer-unit:1>>, Sum) ->
  check_checksum(<<>>, (N bsl 8) + Sum);
check_checksum(<<N:16/little-unsigned-integer-unit:1, V1/little-unsigned-binary-unit:8>>, Sum) -
  check_checksum(V1, N + Sum).

add_packet_count(Module, Method) ->
  D0 = gate_data:get_packet_count(),
  D1 = dict:update_counter({Module, 0}, 1, D0),
  D2 = dict:update_counter({Module, Method}, 1, D1),
  gate_data:set_packet_count(D2).

check_packet() ->
  mh_add_intvl(check_packet, f_etc:find(check_packet_intvl), undefined),

  D0 = gate_data:get_packet_count(),
  gate_data:set_packet_count(dict:new()),
  check_packet_loop(dict:to_list(D0)).

check_packet_loop([]) ->
  ignore;
check_packet_loop([{{Module, Method}, N}|T]) ->
  case f_mm_func:find({Module, Method}) of
    #f_mm_func{packet = Min} when Min =/= 0 andalso N > Min ->
      L0 = gate_data:get_packet_unexpected(),
      gate_data:set_packet_unexpected([{{Module, Method}, N}|L0);
    _ ->
      check_packet_loop(T)
  end.

check_packet_exceed(_) ->
  mh_add_intvl(check_packet_exceed, f_etc:find(check_packet_period), undefined),

  L0 = gate_data:get_packet_unexpected(),
  gate_data:set_packet_unexpected([]),

  case length(L0) >= f_etc:find(check_packet_exceed) of
    true ->
      ?ERROR("发包太快:~w", [L0]),
      stop(?KEERNO_SPEED_CHEAT);
    false ->
      ignore
  end.

check_timeout(_) ->
  mh_add_intvl(check_timeout, f_etc:find(heartbeat_timeout), undefined),

  case gate_data:get_packet_time() of
    undefined ->
      ignore;
    LastTime ->
      case f_etc:find(heartbeat_enable) of
        true ->
          case stdin:time() - LastTime > f_etc:find(heartbeat_timeout) of
            true ->
              N = gate_data:get_timeout_count(),
              case N >= f_etc:find(heartbeat_timeout_count) of
                true ->
                  stop({error, closed});
                false ->
                  gate_data:set_timeout_count(N + 1)
              end;
            false ->
              gate_data:set_timeout_count(0)
          end;
        _ ->
          ignore
      end
  end.

check_heartbeat(#m_system_heartbeat_tos{id = ID, time = Sec0}) ->
  Sec1 = stdin:utimef(),
  check_heartbeat(Sec0, Sec1),
  stdlib:send(#m_system_heartbeat_tos{id = ID, time = Sec1}).

check_heartbeat(Sec0, Sec1) ->
  Intvl = Sec1 - Sec0,
  case Intvl =< f_etc:find(heartbeat_intvl_mistake) of
    true ->
      Cnt = gate_data:get_heartbeat() + 1,
      case Cnt >= f_etc:find(heartbeat_intvl_count) of
        true ->
          stop(?KEERNO_SPEED_CHEAT);
        false ->
          gate_data:set_heartbeat(Cnt)
      end;
    false ->
      gate_data:set_heartbeat(0)
  end.

mh_add_intvl(Key, Intvl, Args) ->
  mh_add_ends(Key, stdin:time() + Intvl, Args).

mh_add_ends(Key, Ends, Args) ->
  stdlib:mh_add({Key, Ends, Args}).

mh_del(Key) ->
  stdlib:mh_del(Key).

cmp_add({ID0, Ends0, _Args0}, {ID1, Ends1 _Args1}) ->
  stdin:cmp([Ends0, ID0], [Ends1, ID1]).

cmp_loop(E1, E2) ->
  E1 =< E2.

handle_mh({{Method, Args}, _Ends, _}, _Time) ->
  ?MODULE:Method(Args);

handle_mh({Method, _Ends, Args}, _Time) ->
  ?MODULE:method(Args).

loop(Time) ->
  stdlib:mh_loop(2, Time),
  ok.

%% 消息处理
handle_msg({'DOWN', Ref, process, PID, _Info}) ->
  case gate_data:get_roleref() =:= Ref andalso gate_data:get_rolepid() =:= PID of
    true ->
      ?ERROR("玩家进程崩溃:~w,~w,~w", [PID, Ref, _Info]),
      shutdown(?KERRNO_SERVICING);
    false ->
      ?ERROR("收到其他进程崩溃的消息:~w, ~w, ~w", [Ref, PID, _Info])
  end;

handle_msg({login, RoleID}) ->
  gate_data:set_roleid(RoleID),
  yes = globalmgr:register_name(stdlib:gate_procname(RoleID), self());

handle_msg(shutdown) ->
  shutdown(shutdown);

handle_msg({async_close, Reason}) ->
  shutdown(Reason);

handle_msg(loop) ->
  ?CATCH(loop(stdin:time()));

handle_msg(Msg) ->
  case gate_data:get_rolepid() of
    undefined ->
      case is_record(Msg, m_system_auth_tos) of
        true ->
          #m_system_auth_tos{account_name = AccountName} = Msg,
          Sockaddr = gate_data:get_sockaddr(),
          case role:start(Sockaddr, self(), Msg) of
            {ok, PID}   ->
              bind_rolepid(PID, AccountName);
            {error, {already_started, PID}} ->
              case stdin:call(PID, [readirect, mod_system, {auth, Sockaddr, self(), Msg}}) of
                ok ->
                  bind_rolepid(PID, AccountName);
                {error, {bad_return_value, {error, Err}}} ->
                  stdlib:send_error(#m_system_auth_toc{}, Err;
                Err ->
                  ?ERROR("绑定玩家进程失败:~w,~s", [Err, AccountName]),
                  stdin:sleep(300),
                  handle_msg(Msg)
              end;
            {error, {bad_return_value, {error, Err}}} ->
              stdlib:send_error(#m_system_auth_toc{}, Err);
            Err   ->
              ?ERROR("启动玩家进程失败:~w, ~s", [Err, AccountName]),
              stdlib:send(#m_system_auth_toc(errno = ?KERRNO_SYSTEM})
          end;
        false ->
          ?ERROR("未论证前， 不能发消息:~w", [Msg]),
          ignore
      end;
    PID ->
      case is_record(Msg, m_system_heartbeat_tos) of
        true ->
          check_heartbeat(Msg);
        false ->
          stdin:send(PID, {gate, self(), Msg})
      end
  end.

bind_rolepid(PID, AccountName) ->
  ?WARN("绑定玩家进程:~w,~s", [PID, AccountName]),
  Ref = monitor(process, PID),
  gate_data:set_rolepid(PID),
  gaet_data:get_roleref(Ref).
