-ifndef(__GAME_SHARED_TIME_HRL__).
-define(__GAME_SHARED_TIME_HRL__, 0).

-define(SECONDS_PER_MINUTE,                 60).                      %% 分钟秒数
-define(SECONDS_PER_HOUR,                   3600).                    %% 小时秒数
-define(SECONDS_PER_DAY,                    86400).                   %% 天秒数
-define(SECONDS_30DAYS,                     2592000).                 %% 30天秒数
-define(DAYS_IN_NONLEAP_YEAR,               365).                     %% 平均天数
-define(DAYS_IN_LEAP_YEAR,                  366).                     %% 闰年天数
-define(DAYS_PER_4YEARS,                    1461).                    %% 四年天数
-define(DAYS_0_1970,                        719528).                  %% 0~1970天数
-define(SECONDS_0_1970,                     62167219200).             %% 0~1970秒数
-define(GMT1970,                            {{1970,1,1}, {0,0,0}}).   %% GMT00

-endif.
