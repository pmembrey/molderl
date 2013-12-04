
-module(molderl).
-behaviour(gen_server).

-export([start_link/1, create_stream/6, send_message/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("molderl.hrl").

-record(state, { streams_sup, streams = [] } ).

% gen_server API

start_link(SupervisorPID) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, SupervisorPID, []).

create_stream(StreamProcessName,StreamName,Destination,DestinationPort,IPAddressToSendFrom,Timer) ->
    gen_server:call(?MODULE,{create_stream,StreamProcessName,StreamName,Destination,DestinationPort,IPAddressToSendFrom,Timer}).

send_message(StreamProcessName,Message) ->
    gen_server:cast(?MODULE,{send,StreamProcessName,Message}).

% gen_server's callbacks

init(SupervisorPID) ->

    % remind yourself to start molderl_stream_supersup
    self() ! {start_molderl_stream_supersup, SupervisorPID},

    {ok, #state{}}.

handle_call({create_stream,StreamProcessName,StreamName,Destination,DestinationPort,IPAddressToSendFrom,Timer},_From,State) ->
    case lists:member(StreamProcessName, State#state.streams) of
        true ->
            {reply, {error, already_exist}, State};
        false ->
            Spec = ?CHILD(make_ref(),
                          molderl_stream_sup,
                          [StreamProcessName,StreamName,Destination,DestinationPort,IPAddressToSendFrom,Timer],
                          transient,
                          supervisor),
            case supervisor:start_child(State#state.streams_sup, Spec) of
                {ok, _Pid} ->
                    {reply, ok, State#state{streams=[StreamProcessName|State#state.streams]}};
                {error, Error} ->
                    {reply, {error, Error},  State}
            end
    end.

handle_cast({send, StreamProcessName, Message}, State) ->
    molderl_stream:send(StreamProcessName, Message),
    {noreply, State}.

handle_info({start_molderl_stream_supersup, SupervisorPID}, State) ->
    Spec = ?CHILD(molderl_stream_supersup, molderl_stream_supersup, [], permanent, supervisor),
    case supervisor:start_child(SupervisorPID, Spec) of
        {ok, StreamsSup} ->
            {noreply, State#state{streams_sup=StreamsSup}};
        {error, {already_started, StreamsSup}}->
            {noreply, State#state{streams_sup=StreamsSup}}
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(normal, _State) ->
    ok.

