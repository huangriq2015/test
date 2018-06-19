-module(vmapmgr).

-behaviour(gne_server).

-export([
    join_sup/1,
    create/1,
    destory/1,
    get_data/2,
    start/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
  ]).
