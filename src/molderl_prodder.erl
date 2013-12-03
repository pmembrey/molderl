
-module(molderl_prodder).
-export([init/2]).

init(MoldStreamPid,Timeout) ->
    % Wait for timeout...
    timer:sleep(Timeout),
    % Send alert to the mold stream process
    MoldStreamPid ! prod,
    % Go back and do it all again...
    init(MoldStreamPid,Timeout).

