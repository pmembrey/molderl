
-module(molderl).
-behaviour(gen_server).

-export([start_link/1, create_stream/6, send_message/2, send_message/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("molderl.hrl").

-record(state, { streams_sup, streams = [] } ).

% gen_server API

start_link(SupervisorPID) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, SupervisorPID, []).

create_stream(StreamName,Destination,DestinationPort,RecoveryPort,IPAddressToSendFrom,Timer) ->
    gen_server:call(?MODULE,{create_stream,StreamName,Destination,DestinationPort,RecoveryPort,IPAddressToSendFrom,Timer}).

send_message(StreamName, Message) ->
    gen_server:cast(?MODULE, {send, StreamName, Message, os:timestamp()}).

% Third argument, StartTime, is only for the user to
% manually supply a start time {MacroSecs, Secs, MicroSecs}
% on which the latency published by StatsD will be based on
send_message(StreamName, Message, StartTime) ->
    gen_server:cast(?MODULE, {send, StreamName, Message, StartTime}).

% gen_server's callbacks

init(SupervisorPID) ->

    % remind yourself to start molderl_stream_supersup
    self() ! {start_molderl_stream_supersup, SupervisorPID},

    {ok, #state{}}.

handle_call({create_stream,StreamName,Destination,DestinationPort,RecoveryPort,IPAddressToSendFrom,Timer},_From,State) ->
    % before creating MOLD stream, make sure there's no stream name, destination address or recovery port conflict
    case {lists:keymember(StreamName,1,State#state.streams),
          lists:keymember({Destination,DestinationPort},2,State#state.streams),
          lists:keymember(RecoveryPort,3,State#state.streams)} of
        {false,false,false} -> % stream name, destination addr and recovery port available
            Spec = ?CHILD(make_ref(),
                          molderl_stream_sup,
                          [StreamName,Destination,DestinationPort,RecoveryPort,IPAddressToSendFrom,Timer],
                          transient,
                          supervisor),
            case supervisor:start_child(State#state.streams_sup, Spec) of
                {ok, _Pid} ->
                    {reply, ok, State#state{streams=[{StreamName,{Destination,DestinationPort},RecoveryPort}|State#state.streams]}};
                {error, Error} ->
                    {reply, {error, Error},  State}
            end;
        {true,_,_} ->
            {reply, {error, already_exist}, State};
        {_,true,_} ->
            {reply, {error, eaddrinuse}, State};
        {_,_,true} ->
            {reply, {error, eaddrinuse}, State}
    end.

handle_cast({send, StreamName, Message, StartTime}, State) ->
    molderl_stream:send(StreamName, Message, StartTime),
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

