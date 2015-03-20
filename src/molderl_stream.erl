
-module(molderl_stream).

-behaviour(gen_server).

-export([start_link/1, send/3, set_sequence_number/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("molderl.hrl").

-compile([{parse_transform, lager_transform}]).

-type message() :: binary(). % a single binary received and to be multicasted
-type packet() :: {erlang:timestamp(), [message()]}. % a list of messages of the right size to be multicasted

-record(state, {stream_name :: binary(),                  % Name of the stream encoded for MOLD64 (i.e. padded binary)
                destination :: inet:ip4_address(),        % The IP address to send / broadcast / multicast to
                sequence_number :: pos_integer(),         % Next sequence number
                socket :: inet:socket(),                  % The socket to send the data on
                destination_port :: inet:port_number(),   % Destination port for the data
                packets = [] :: [packet()],               % List of packets ready to be encoded and sent
                messages = {{0,0,0}, []} :: packet(),     % List of msgs waiting to be long enough to be added to packets
                buffer_size = 0 :: non_neg_integer(),     % Current length of buffered messages if they were
                                                          % to be encoded in a MOLD64 packet
                recovery_service :: pid() ,               % Pid of the recovery service message
                prod_interval :: pos_integer(),           % Maximum interval at which either partial packets
                                                          % or heartbeats should be sent
                timer_ref :: reference(),                 % reference to timer used for hearbeats and flush interval
                statsd_latency_key_in :: string(),        %
                statsd_latency_key_out :: string(),       % cache the StatsD keys to prevent binary_to_list/1 calls
                statsd_count_key :: string()              % and concatenation all the time
               }).

start_link(Arguments) ->
    gen_server:start_link(?MODULE, Arguments, []).

-spec send(pid(), binary(), erlang:timestamp()) -> 'ok'.
send(Pid, Message, StartTime) ->
    gen_server:cast(Pid, {send, Message, StartTime}).

-spec set_sequence_number(pid(), pos_integer()) -> 'ok'.
set_sequence_number(Pid, SeqNum) ->
    gen_server:cast(Pid, {sequence_number, SeqNum}).

init(Arguments) ->

    {streamname, StreamName} = lists:keyfind(streamname, 1, Arguments),
    {destination, Destination} = lists:keyfind(destination, 1, Arguments),
    {destinationport, DestinationPort} = lists:keyfind(destinationport, 1, Arguments),
    {ipaddresstosendfrom, IPAddressToSendFrom} = lists:keyfind(ipaddresstosendfrom, 1, Arguments),
    {timer, ProdInterval} = lists:keyfind(timer, 1, Arguments),
    {multicast_ttl, TTL} = lists:keyfind(multicast_ttl, 1, Arguments),

    process_flag(trap_exit, true), % so that terminate/2 gets called when process exits

    % send yourself a reminder to start recovery process
    RecoveryArguments = [{mold_stream, self()}|[{packetsize, ?PACKET_SIZE}|Arguments]],
    self() ! {initialize, RecoveryArguments},

    Connection = gen_udp:open(0, [binary,
                                  {broadcast, true},
                                  {ip, IPAddressToSendFrom},
                                  {add_membership, {Destination, IPAddressToSendFrom}},
                                  {multicast_if, IPAddressToSendFrom},
                                  {multicast_ttl, TTL},
                                  {reuseaddr, true}]),

    case Connection of
        {ok, Socket} ->
            State = #state{stream_name = molderl_utils:gen_streamname(StreamName),
                           destination = Destination,
                           socket = Socket,
                           destination_port = DestinationPort,
                           timer_ref = erlang:send_after(ProdInterval, self(), prod),
                           prod_interval = ProdInterval,
                           statsd_latency_key_in = "molderl." ++ atom_to_list(StreamName) ++ ".time_in",
                           statsd_latency_key_out = "molderl." ++ atom_to_list(StreamName) ++ ".time_out",
                           statsd_count_key = "molderl." ++ atom_to_list(StreamName) ++ ".packets_sent"},
            {ok, State};
        {error, Reason} ->
            lager:error("[molderl] Unable to open UDP socket on ~p because '~p'. Aborting.",
                        [IPAddressToSendFrom, inet:format_error(Reason)]),
            {stop, Reason}
    end.

handle_call(Msg, _From, State) ->
    lager:warning("[molderl] Unexpected message in module ~p: ~p",[?MODULE, Msg]),
    {noreply, State}.

% first check if msg is too big for single packet
handle_cast({send, Msg, _StartTime}, State) when byte_size(Msg)+22 > ?PACKET_SIZE ->
    Log = "[molderl] Received a single message of length ~p which is bigger than the maximum packet size ~p. Ignoring.",
    lager:error(Log, [byte_size(Msg), ?PACKET_SIZE]),
    {noreply, State};

% second handle a msg when there's no messages in buffer
handle_cast({send, Msg, StartTime}, OldState=#state{messages={_,[]}}) ->
    State = OldState#state{messages={StartTime, [Msg]}, buffer_size=byte_size(Msg)+22},
    case flush(State) of
        {ok, NewState} ->
            {noreply, NewState};
        {error, Reason} ->
            {stop, Reason, State}
    end;

% third handle if the msg is big enough to promotes the current msgs buffer to a packet
handle_cast({send, Msg, Start}, State=#state{packets=Pckts, messages=Msgs, buffer_size=Size})
        when Size+byte_size(Msg)+2 > ?PACKET_SIZE ->
    handle_cast({send, Msg, Start}, State#state{packets=[Msgs|Pckts], messages={Start, []}, buffer_size=0});

% finally handle if the msg is not big enough to promotes the current msgs buffer to a packet
handle_cast({send, Msg, Start}, OldState=#state{messages={_, Msgs}, buffer_size=Size}) ->
    State = OldState#state{messages={Start, [Msg|Msgs]}, buffer_size=Size+byte_size(Msg)+2},
    case flush(State) of
        {ok, NewState} ->
            {noreply, NewState};
        {error, Reason} ->
            {stop, Reason, State}
    end;

handle_cast({sequence_number, SeqNum}, State) ->
    {noreply, State#state{sequence_number=SeqNum}}.

handle_info(prod, State=#state{packets=[], messages={_,[]}}) ->
    % Timer triggered a send, but packets/msgs queue empty
    send_heartbeat(State),
    TRef = erlang:send_after(State#state.prod_interval, self(), prod),
    {noreply, State#state{timer_ref=TRef}};

handle_info(prod, OldState=#state{packets=Pckts, messages=Pckt}) ->
    % Timer triggered a send, flush packets/msgs buffer
    State = OldState#state{packets=[Pckt|Pckts], messages={{0,0,0}, []}},
    case flush(State) of
        {ok, NewState} ->
            {noreply, NewState};
        {error, Reason} ->
            {stop, Reason, State}
    end;

handle_info({initialize, Arguments}, State) ->
    {supervisorpid, SupervisorPID} = lists:keyfind(supervisorpid, 1, Arguments),
    RecoverySpec = ?CHILD(make_ref(), molderl_recovery, [Arguments], transient, worker),
    {ok, RecoveryProcess} = supervisor:start_child(SupervisorPID, RecoverySpec),
    {noreply, State#state{recovery_service=RecoveryProcess}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, State) ->
    ok = gen_udp:close(State#state.socket),
    Fmt = "[molderl] molderl_stream process for stream ~p is exiting because of reason ~p.",
    lager:info(Fmt, [string:strip(binary_to_list(State#state.stream_name)), Reason]),
    ok.

-spec flush(#state{}) -> {'ok', #state{}} | {'error', inet:posix()}.
flush(State=#state{packets=[]}) ->
    % no packets to send, nothing to do
    TRef = erlang:send_after(State#state.prod_interval, self(), prod),
    {ok, State#state{timer_ref=TRef}};

flush(State=#state{sequence_number=undefined}) ->
    % can't send messages out because we don't know our sequence number yet
    {ok, State};

flush(State=#state{packets=[{_Start, []}|Pckts]}) -> % empty packet, ignore and go on
    flush(State#state{packets=Pckts});

flush(State=#state{packets=[{Start, Msgs}|Pckts], sequence_number=SeqNum}) ->
    % send out the packets in the current queue
    erlang:cancel_timer(State#state.timer_ref),
    {EncodedMsgs, EncodedMsgsSize, NumMsgs} = molderl_utils:encode_messages(Msgs),
    Payload = molderl_utils:gen_messagepacket(State#state.stream_name, SeqNum, NumMsgs, EncodedMsgs),
    case gen_udp:send(State#state.socket, State#state.destination, State#state.destination_port, Payload) of
        ok ->
            molderl_recovery:store(State#state.recovery_service, EncodedMsgs, EncodedMsgsSize, NumMsgs),
            statsderl:timing_now(State#state.statsd_latency_key_out, Start, 0.1),
            statsderl:increment(State#state.statsd_count_key, 1, 0.1),
            flush(State#state{packets=Pckts, sequence_number=SeqNum+NumMsgs});
        {error, eagain} -> % retry next cycle
            TRef = erlang:send_after(State#state.prod_interval, self(), prod),
            {ok, State#state{timer_ref=TRef}};
        {error, Reason} ->
            Log = "[molderl] Experienced issue ~p (~p) writing to UDP socket. Resetting.",
            lager:error(Log, [Reason, inet:format_error(Reason)]),
            {error, Reason}
    end.

-spec send_heartbeat(#state{}) -> 'ok' | {'error', inet:posix() | 'not_owner'}.
send_heartbeat(State=#state{stream_name=Name, socket=Socket, destination=Destination}) ->
    Heartbeat = molderl_utils:gen_heartbeat(Name, State#state.sequence_number),
    gen_udp:send(Socket, Destination, State#state.destination_port, Heartbeat).

%send_endofsession(State) ->
%    EndOfSession = molderl_utils:gen_endofsession(State#state.stream_name, State#state.sequence_number),
%    gen_udp:send(State#state.socket, State#state.destination, State#state.destination_port, EndOfSession).

