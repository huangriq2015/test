-ifndef(__GAME_SHARED_TCP_HRL__).
-define(__GAME_SHARED_TCP_HRL__, 0).

-define(TCP_LISTEN_OPTIONS, [{reuseaddr, true}, {backlog, 384}]).
-define(TCP_CONNECT_OPTIONS, [binary, {active,false}, {packet, raw}, {packet_size, 1048576}, {linger, {true, 0}},{recbuf, 8092},{sndbuf, 65536}, {nodelay, true}, {delay_send, false}, {keepalive, true}, {exit_on_close, true}]).

-endif. %%(__GAME_SHARED_TCP_HRL__)
