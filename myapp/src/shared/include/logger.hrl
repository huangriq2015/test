-ifndef(__GAME_SHARED_LOGGER_HRL__).
-define(__GAME_SHARED_LOGGER_HRL__, 0).

-define(ERROR(Msg), logger:write(error, "~s ~s" ++ Msg,[pid_to_list(self()), lists:concat([?MODULE, ".erl:", ?LINE])])).
-define(ERROR(Msg, Args), logger:write(error, "~s ~s" ++ Msg,[pid_to_list(self()), lists:concat([?MODULE, ".erl:", ?LINE, Args])])).
    
-define(WARN(Msg), logger:write(warn, "~s ~s" ++ Msg,[pid_to_list(self()), lists:concat([?MODULE, ".erl:", ?LINE])])).
-define(WARN(Msg, Args), logger:write(warn, "~s ~s" ++ Msg,[pid_to_list(self()), lists:concat([?MODULE, ".erl:", ?LINE, Args])])).
    
-define(INFO(Msg), logger:write(info, "~s ~s" ++ Msg,[pid_to_list(self()), lists:concat([?MODULE, ".erl:", ?LINE])])).
-define(INFO(Msg, Args), logger:write(info, "~s ~s" ++ Msg,[pid_to_list(self()), lists:concat([?MODULE, ".erl:", ?LINE, Args])])).

-define(DEBUG(Msg), logger:write(debug, "~s ~s" ++ Msg,[pid_to_list(self()), lists:concat([?MODULE, ".erl:", ?LINE])])).
-define(DEBUG(Msg, Args), logger:write(debug, "~s ~s" ++ Msg,[pid_to_list(self()), lists:concat([?MODULE, ".erl:", ?LINE, Args])])).

-endif. %% __GAME_SHARED_LOGGER_HRL__
