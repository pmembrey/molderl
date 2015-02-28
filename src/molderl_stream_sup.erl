
-module(molderl_stream_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-include("molderl.hrl").

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Arguments) ->
    supervisor:start_link(?MODULE, Arguments).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(Args) ->
    Stream = ?CHILD(make_ref(), molderl_stream, [[{supervisorpid, self()}|Args]], transient, worker),
    {ok, {{one_for_all, 5, 10}, [Stream]}}.

