-ifndef(__GAME_SHARED_STDIN_HRL__).
-define(__GAME_SHARED_STDIN_HRL__, 0).

-include("logger.hrl").

-define(SEL(C, A, B), case (C) of true -> (A); false -> (B) end).

-define(CASE3(X1, X2, A, B, C), if X1 > X2 -> A; X1 < X2 -> B; true -> C end).

-define(CATCH(Fun), fun() -> try Fun catch __ERRTYPE__:__ERRVAL__ -> ?ERROR("CATCH:~w,~w,~w", [__ERRTYPE__, __ERRVAL__, erlang:get_stackstrace()]) end end()).
-define(CATCH_RETURN(Fun), fun() -> try Fun catch __ERRTYPE__:__ERRVAL__ -> __ERRVAL__ end end()).

-define(LIST_AND_EMPTY(S),    (is_list(S) andalso S =:= [])).
-define(LIST_AND_NO_EMPTY(S), (is_list(S) andalso S =/= [])).

-define(FALSE, 0).
-define(TRUE,  1).

-endif. %%(__GAME_SHARED_STDIN_HRL__)
