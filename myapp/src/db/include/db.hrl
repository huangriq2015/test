-ifndef(__GAME_DB_DB_HRL__).
-define(__GAME_DB_DB_HRL__, 0).

%% 数据缓存连接池
-define(POOL_CNT, 4).

%% 写入缓存到数据库时机
%% 1 超时
%% 2 数据满
-define(TIMEOUT,        16000).
-define(DATACTN,        800).

%% 清理数据
-define(DEL_TIMEOUT,    60000).

%% MYSQL超时时间
-define(MYSQL_TIMEOUT,  8000).
-define(SELECT_TIMEOUT, 120000).
-define(BATCH_INSERT_TIMEOUT, 16000).
-define(TRANSACTION_TIMEOUT,  16000).

%% 内嵌表结构
%% {loaded, #tab}         => boolean().
%% {match, $tab, #match}  => boolean().
%% {auto_increment, $tab} => integer().

-record(t_builtin, {key, value}).

%% 表结构
-record(table_info, {db, fields, join_fields, sql_id}).

%% 日志表初始化
-record(table_pools, {table, id}).

-endif.
