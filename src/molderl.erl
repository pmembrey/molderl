
-module(molderl).
-behaviour(gen_server).

-export([start_link/1, create_stream/7, send_message/2, send_message/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("molderl.hrl").

-compile([{parse_transform, lager_transform}]).

-record(stream, {destination_addr :: {inet:ip4_address(), inet:port_number()},
                 recovery_port :: inet:port_number()}).

-record(state, {streams_sup :: pid() , streams = [] :: [#stream{}]} ).

% gen_server API

start_link(SupervisorPID) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, SupervisorPID, []).

-spec create_stream(atom(), inet:ip4_address(), inet:port_number(), inet:port_number(), inet:ip_address(), pos_integer(), pos_integer())
    -> {'ok', pid()} | {'error', atom()}.
create_stream(StreamName,Destination,DestinationPort,RecoveryPort,IPAddressToSendFrom,Timer,TTL) ->
    gen_server:call(?MODULE,{create_stream,StreamName,Destination,DestinationPort,RecoveryPort,IPAddressToSendFrom,Timer,TTL}).

-spec send_message(pid(), binary()) -> 'ok'.
send_message(Stream, Message) ->
    gen_server:cast(?MODULE, {send, Stream, Message, os:timestamp()}).

% Third argument, StartTime, is only for the user to
% manually supply a start time {MacroSecs, Secs, MicroSecs}
% on which the latency published by StatsD will be based on
send_message(Stream, Message, StartTime) ->
    gen_server:cast(?MODULE, {send, Stream, Message, StartTime}).

% gen_server's callbacks

init(SupervisorPID) ->

    % remind yourself to start molderl_stream_supersup
    self() ! {start_molderl_stream_supersup, SupervisorPID},

    {ok, #state{}}.

handle_call({create_stream,StreamName,Destination,DestinationPort,RecoveryPort,IPAddressToSendFrom,Timer,TTL},_From,State) ->
    case conflict_check(Destination, DestinationPort, RecoveryPort, State#state.streams) of
        ok ->
            Spec = ?CHILD(make_ref(),
                          molderl_stream_sup,
                          [StreamName,Destination,DestinationPort,RecoveryPort,IPAddressToSendFrom,Timer,TTL],
                          transient,
                          supervisor),
            case supervisor:start_child(State#state.streams_sup, Spec) of
                {ok, Pid} ->
                    {_, StreamPid, _, _} = lists:keyfind([molderl_stream], 4, supervisor:which_children(Pid)),
                    Stream = #stream{destination_addr={Destination,DestinationPort}, recovery_port=RecoveryPort},
                    {reply, {ok, StreamPid}, State#state{streams=[Stream|State#state.streams]}};
                {error, Error} ->
                    {reply, {error, Error}, State}
            end;
        {error, Error} ->
            lager:error("[molderl] Unable to create stream '~p' because '~p'", [StreamName, Error]),
            {reply, {error, Error},  State}
    end.

handle_cast({send, Stream, Message, StartTime}, State) ->
    molderl_stream:send(Stream, Message, StartTime),
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

% Make sure there's no destination address or recovery port conflict
-spec conflict_check(inet:ip4_address(), inet:port_number(), inet:port_number(), [#stream{}])
    -> 'ok' | {'error', atom()}.
conflict_check(Destination, DestinationPort, RecoveryPort, Streams) ->
    case {lists:any(fun(S) -> S#stream.destination_addr =:= {Destination, DestinationPort} end, Streams),
          lists:any(fun(S) -> S#stream.recovery_port =:= RecoveryPort end, Streams)} of
        {false, false} ->
            ok;
        {true, _} ->
            {error, destination_address_already_in_use};
        {_, true} ->
            {error, recovery_port_already_in_use}
    end.

