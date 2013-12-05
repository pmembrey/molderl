
-module(molderl_prodder).
-export([start_link/2]).
-export([init/2]).

start_link(MoldStreamPid, TimeOut) ->
    proc_lib:start_link(?MODULE, init, [MoldStreamPid, TimeOut]).

init(MoldStreamPid, TimeOut) ->
    % acknowledge spawning to supervisor
    proc_lib:init_ack({ok, self()}), 
    loop(MoldStreamPid, TimeOut).

loop(MoldStreamPid, TimeOut) ->
    % Wait for timeout...
    timer:sleep(TimeOut),
    % Send alert to the mold stream process
    molderl_stream:prod(MoldStreamPid),
    % Go back and do it all again...
    loop(MoldStreamPid, TimeOut).

