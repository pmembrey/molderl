
-module(molderl_stream).

-behaviour(gen_server).

-export([start_link/1, send/3, set_sequence_number/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("molderl.hrl").

-compile([{parse_transform, lager_transform}]).

-type message() :: binary(). % a single binary received and to be multicasted
-type packet() :: {erlang:timestamp(), [message()]}. % a list of messages of the right size to be multicasted

-record(info, {stream_name :: binary(),                  % Name of the stream encoded for MOLD64 (i.e. padded binary)
               destination :: inet:ip4_address(),        % The IP address to send / broadcast / multicast to
               socket :: inet:socket(),                  % The socket to send the data on
               destination_port :: inet:port_number(),   % Destination port for the data
                                                         % to be encoded in a MOLD64 packet
               recovery_service :: pid() ,               % Pid of the recovery service message
               prod_interval :: pos_integer(),           % Maximum interval at which either partial packets
                                                         % or heartbeats should be sent
               statsd_latency_key_in :: string(),        %
               statsd_latency_key_out :: string(),       % cache the StatsD keys to prevent binary_to_list/1 calls
               statsd_count_key :: string()              % and concatenation all the time
              }).

-record(state, {sequence_number :: pos_integer(),         % Next sequence number
                packets = [] :: [packet()],               % List of packets ready to be encoded and sent
                messages = {{0,0,0}, []} :: packet(),     % List of msgs waiting to be long enough to be added to packets
                buffer_size = 0 :: non_neg_integer(),     % Current length of buffered messages if they were
                                                          % to be encoded in a MOLD64 packet
                timer_ref :: reference()                  % reference to timer used for hearbeats and flush interval
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
            Info = #info{stream_name = molderl_utils:gen_streamname(StreamName),
                         destination = Destination,
                         socket = Socket,
                         destination_port = DestinationPort,
                         prod_interval = ProdInterval,
                         statsd_latency_key_in = "molderl." ++ atom_to_list(StreamName) ++ ".time_in",
                         statsd_latency_key_out = "molderl." ++ atom_to_list(StreamName) ++ ".time_out",
                         statsd_count_key = "molderl." ++ atom_to_list(StreamName) ++ ".packets_sent"},
            State = #state{timer_ref = erlang:send_after(ProdInterval, self(), prod)},
            {ok, {Info, State}};
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
handle_cast({send, Msg, StartTime}, {Info, OldState=#state{messages={_,[]}}}) ->
    State = OldState#state{messages={StartTime, [Msg]}, buffer_size=byte_size(Msg)+22},
    {noreply, {Info, State}};

% third handle if the msg is big enough to promotes the current msgs buffer to a packet
handle_cast({send, Msg, Start}, {Info, OldState=#state{packets=Pckts, messages=Msgs, buffer_size=Size}})
        when Size+byte_size(Msg)+2 > ?PACKET_SIZE ->
    State = OldState#state{packets=[Msgs|Pckts], messages={Start, [Msg]}, buffer_size=byte_size(Msg)+22},
    case flush(Info, State) of
        {ok, NewState} ->
            {noreply, {Info, NewState}};
        {error, Reason} ->
            {stop, Reason, {Info, State}}
    end;

% finally handle if the msg is not big enough to promotes the current msgs buffer to a packet
handle_cast({send, Msg, _}, {Info, OldState=#state{messages={Start, Msgs}, buffer_size=Size}}) ->
    State = OldState#state{messages={Start, [Msg|Msgs]}, buffer_size=Size+byte_size(Msg)+2},
    {noreply, {Info, State}};

handle_cast({sequence_number, SeqNum}, {Info, State}) ->
    {noreply, {Info, State#state{sequence_number=SeqNum}}}.

handle_info(prod, {Info, State=#state{sequence_number=undefined}}) ->
    % can't send heartbeats out because we don't know our sequence number yet
    TRef = erlang:send_after(Info#info.prod_interval, self(), prod),
    {noreply, {Info, State#state{timer_ref=TRef}}};

handle_info(prod, {Info, State=#state{packets=[], messages={_,[]}}}) ->
    % Timer triggered a send, but packets/msgs queue empty
    send_heartbeat(Info, State#state.sequence_number),
    TRef = erlang:send_after(Info#info.prod_interval, self(), prod),
    {noreply, {Info, State#state{timer_ref=TRef}}};

handle_info(prod, {Info, OldState=#state{packets=Pckts, messages=Msgs}}) ->
    % Timer triggered a send, flush packets/msgs buffer
    State = OldState#state{packets=[Msgs|Pckts], messages={{0,0,0}, []}, buffer_size=0},
    case flush(Info, State) of
        {ok, NewState} ->
            {noreply, {Info, NewState}};
        {error, Reason} ->
            {stop, Reason, {Info, State}}
    end;

handle_info({initialize, Arguments}, {Info, State}) ->
    {supervisorpid, SupervisorPID} = lists:keyfind(supervisorpid, 1, Arguments),
    RecoverySpec = ?CHILD(make_ref(), molderl_recovery, [Arguments], transient, worker),
    {ok, RecoveryProcess} = supervisor:start_child(SupervisorPID, RecoverySpec),
    {noreply, {Info#info{recovery_service=RecoveryProcess}, State}};

handle_info(Info, State) ->
    lager:error("[molderl] molderl_stream:handle_info received unexpected message. Info:~p, State:~p.~n", [Info, State]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, {Info, _State}) ->
    ok = gen_udp:close(Info#info.socket),
    Fmt = "[molderl] molderl_stream process for stream ~p is exiting because of reason ~p.",
    lager:info(Fmt, [string:strip(binary_to_list(Info#info.stream_name)), Reason]),
    ok.

-spec flush(#info{}, #state{}) -> {'ok', #state{}} | {'error', inet:posix()}.
flush(Info, State=#state{sequence_number=undefined}) ->
    % can't send messages out because we don't know our sequence number yet
    % Asynchronous erlang:cancel_timer/2 is only supported in ERTS 7...
%    erlang:cancel_timer(State#state.timer_ref, [{async, true}]),
    TRef = erlang:send_after(Info#info.prod_interval, self(), prod),
    {ok, State#state{timer_ref=TRef}};

flush(Info=#info{prod_interval=Interval}, State=#state{packets=Pckts}) ->
    % Asynchronous erlang:cancel_timer/2 is only supported in ERTS 7...
%    erlang:cancel_timer(State#state.timer_ref, [{async, true}]),
    TRef = erlang:send_after(Interval, self(), prod),
    case flush(Info, State#state.sequence_number, lists:reverse(Pckts)) of
        {ok, SeqNum, UnsentPckts} ->
            {ok, State#state{sequence_number=SeqNum, packets=UnsentPckts, timer_ref=TRef}};
        {error, Error} ->
            {error, Error}
    end.

-spec flush(#info{}, non_neg_integer(), [packet()]) ->
    {'ok', non_neg_integer(), [packet()]} | {'error', inet:posix()}.
flush(_Info, SeqNum, []) ->
    {ok, SeqNum, []};

flush(Info, SeqNum, [{_Start, []}|Pckts]) -> % empty packet, ignore and go on
    flush(Info, SeqNum, Pckts);

flush(Info=#info{stream_name=Name, socket=Socket}, SeqNum, [{Start, Msgs}|Pckts]) ->
    {EncodedMsgs, EncodedMsgsSize, NumMsgs} = molderl_utils:encode_messages(Msgs),
    Payload = molderl_utils:gen_messagepacket(Name, SeqNum, NumMsgs, EncodedMsgs),
    case gen_udp:send(Socket, Info#info.destination, Info#info.destination_port, Payload) of
        ok ->
            molderl_recovery:store(Info#info.recovery_service, EncodedMsgs, EncodedMsgsSize, NumMsgs),
            statsderl:timing_now(Info#info.statsd_latency_key_out, Start, 0.1),
            statsderl:increment(Info#info.statsd_count_key, 1, 0.1),
            flush(Info, SeqNum+NumMsgs, Pckts);
        {error, eagain} -> % retry next cycle
            lager:error("[molderl] Error sending UDP packets: (eagain) resource temporarily unavailable'. Stream:~p. Retrying...~n", [Name]),
            {ok, SeqNum, lists:reverse([{Start, Msgs}|Pckts])};
        {error, Reason} ->
            Log = "[molderl] Experienced issue ~p (~p) writing to UDP socket. Resetting.",
            lager:error(Log, [Reason, inet:format_error(Reason)]),
            {error, Reason}
    end.

-spec send_heartbeat(#info{}, non_neg_integer()) -> 'ok' | {'error', inet:posix() | 'not_owner'}.
send_heartbeat(Info=#info{socket=Socket, destination=Destination}, SeqNum) ->
    Heartbeat = molderl_utils:gen_heartbeat(Info#info.stream_name, SeqNum),
    gen_udp:send(Socket, Destination, Info#info.destination_port, Heartbeat).

%send_endofsession(State) ->
%    EndOfSession = molderl_utils:gen_endofsession(State#state.stream_name, State#state.sequence_number),
%    gen_udp:send(State#state.socket, State#state.destination, State#state.destination_port, EndOfSession).

