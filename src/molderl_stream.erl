
-module(molderl_stream).

-behaviour(gen_server).

-export([start_link/1, send/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("molderl.hrl").

-compile([{parse_transform, lager_transform}]).

-define(STATE,State#state).

-record(state, {stream_name :: binary(),                 % Name of the stream encoded for MOLD64 (i.e. padded binary)
                destination :: inet:ip4_address(),       % The IP address to send / broadcast / multicast to
                sequence_number :: pos_integer(),        % Next sequence number
                socket :: inet:socket(),                 % The socket to send the data on
                destination_port :: inet:port_number(),  % Destination port for the data
                messages = [] :: [binary()],             % List of messages waiting to be encoded and sent
                message_length = 0 :: non_neg_integer(), % Current length of messages if they were to be encoded in a MOLD64 packet
                recovery_service :: pid() ,              % Pid of the recovery service message
                start_time :: erlang:timestamp(),        % Start time of the earliest msg in a packet
                prod_interval :: pos_integer(),          % Maximum interval at which either partial packets or heartbeats should be sent
                timer_ref :: reference(),                % reference to timer used for hearbeats and flush interval
                statsd_latency_key_in :: string(),       %
                statsd_latency_key_out :: string(),      % cache the StatsD keys to prevent binary_to_list/1 calls
                statsd_count_key :: string(),            % and concatenation all the time
                statsd_memory_key :: string()            %
               }).

start_link(Arguments) ->
    gen_server:start_link(?MODULE, Arguments, []).

-spec send(pid(), binary(), erlang:timestamp()) -> 'ok'.
send(Pid, Message, StartTime) ->
    gen_server:cast(Pid, {send, Message, StartTime}).

init(Arguments) ->

    {streamname, StreamName} = lists:keyfind(streamname, 1, Arguments),
    {filename, FileName} = lists:keyfind(filename, 1, Arguments),
    {destination, Destination} = lists:keyfind(destination, 1, Arguments),
    {destinationport, DestinationPort} = lists:keyfind(destinationport, 1, Arguments),
    {ipaddresstosendfrom, IPAddressToSendFrom} = lists:keyfind(ipaddresstosendfrom, 1, Arguments),
    {timer, ProdInterval} = lists:keyfind(timer, 1, Arguments),
    {multicast_ttl, TTL} = lists:keyfind(multicast_ttl, 1, Arguments),

    process_flag(trap_exit, true), % so that terminate/2 gets called when process exits

    case load_store(FileName) of
        {ok, FileSize, Index} ->

            % send yourself a reminder to start recovery process
            RecoveryArguments = [{filesize, FileSize}|[{index, Index}|[{packetsize, ?PACKET_SIZE}|Arguments]]],
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
                                   sequence_number = length(Index)+1,
                                   socket = Socket,
                                   destination_port = DestinationPort,
                                   timer_ref = erlang:send_after(ProdInterval, self(), prod),
                                   prod_interval = ProdInterval,
                                   statsd_latency_key_in = "molderl." ++ atom_to_list(StreamName) ++ ".time_in",
                                   statsd_latency_key_out = "molderl." ++ atom_to_list(StreamName) ++ ".time_out",
                                   statsd_count_key = "molderl." ++ atom_to_list(StreamName) ++ ".packets_sent",
                                   statsd_memory_key = "molderl." ++ atom_to_list(StreamName) ++ ".bytes_sent"},
                    {ok, State};
                {error, Reason} ->
                    lager:error("[molderl] Unable to open UDP socket on ~p because '~p'. Aborting.",
                              [IPAddressToSendFrom, inet:format_error(Reason)]),
                    {stop, Reason}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

handle_cast({send, Message, StartTime}, State=#state{messages=[]}) -> % first msg on the queue
    statsderl:timing_now(?STATE.statsd_latency_key_in, StartTime, 0.1),
    MessageLength = molderl_utils:message_length(0, Message),
    % first, check if single message is bigger than packet size
    case MessageLength > ?PACKET_SIZE of
        true -> % log error, ignore message, but continue
            lager:error("[molderl] Received a single message of length ~p"
                        ++ " which is bigger than the maximum packet size ~p",
                        [MessageLength, ?PACKET_SIZE]),
            {noreply, State};
        false ->
            {noreply, ?STATE{message_length=MessageLength, messages=[Message], start_time=StartTime}}
    end;
handle_cast({send, Message, StartTime}, State) ->
    % Can we fit this in?
    MessageLength = molderl_utils:message_length(?STATE.message_length, Message),
    case MessageLength > ?PACKET_SIZE of
        true -> % Nope we can't, send what we have and requeue
            erlang:cancel_timer(?STATE.timer_ref),
            case flush(State) of
                {ok, NewState} ->
                    % reprocess msg now that we have clean buffer
                    handle_cast({send, Message, StartTime}, NewState);
                {error, Reason} ->
                    {stop, Reason, State}
            end;
        false -> % Yes we can - add it to the list of messages
            {noreply, ?STATE{message_length=MessageLength, messages=[Message|?STATE.messages]}}
    end.

handle_info({initialize, Arguments}, State) ->
    {supervisorpid, SupervisorPID} = lists:keyfind(supervisorpid, 1, Arguments),
    RecoverySpec = ?CHILD(make_ref(), molderl_recovery, [Arguments], transient, worker),
    {ok, RecoveryProcess} = supervisor:start_child(SupervisorPID, RecoverySpec),
    {noreply, ?STATE{recovery_service=RecoveryProcess}};
handle_info(prod, State=#state{messages=[]}) -> % Timer triggered a send, but msg queue empty
    send_heartbeat(State),
    TRef = erlang:send_after(?STATE.prod_interval, self(), prod),
    {noreply, ?STATE{message_length=0, messages=[], timer_ref=TRef}};
handle_info(prod, State) -> % Timer triggered a send
    case flush(State) of
        {ok, NewState} ->
            {noreply, NewState};
        {error, Reason} ->
            {stop, Reason, State}
    end.

handle_call(Msg, _From, State) ->
    lager:warning("[molderl] Unexpected message in module ~p: ~p",[?MODULE, Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, State) ->
    ok = gen_udp:close(State#state.socket),
    Fmt = "[molderl] molderl_stream process for stream ~p is exiting because of reason ~p.",
    lager:info(Fmt, [string:strip(binary_to_list(State#state.stream_name)), Reason]),
    ok.

-spec flush(#state{}) -> {'ok', #state{}} | {'error', inet:posix()}.
flush(State) -> % send out the messages in the current buffer queue
    {EncodedMsgs, EncodedMsgsSize, NumMsgs, NumBytes} = molderl_utils:encode_messages(?STATE.messages),
    Payload = molderl_utils:gen_messagepacket(?STATE.stream_name, ?STATE.sequence_number, NumMsgs, EncodedMsgs),
    case gen_udp:send(?STATE.socket, ?STATE.destination, ?STATE.destination_port, Payload) of
        ok ->
            molderl_recovery:store(?STATE.recovery_service, EncodedMsgs, EncodedMsgsSize, NumMsgs),
            statsderl:timing_now(?STATE.statsd_latency_key_out, ?STATE.start_time, 0.1),
            statsderl:increment(?STATE.statsd_count_key, 1, 0.1),
            statsderl:increment(?STATE.statsd_memory_key, NumBytes, 0.1),
            TRef = erlang:send_after(?STATE.prod_interval, self(), prod),
            {ok, ?STATE{message_length=0, messages=[], sequence_number=?STATE.sequence_number+NumMsgs, timer_ref=TRef}};
        {error, eagain} ->
            TRef = erlang:send_after(?STATE.prod_interval, self(), prod),
            {ok, ?STATE{timer_ref=TRef}}; % retry next cycle
        {error, Reason} ->
            lager:error("[molderl] Experienced issue ~p (~p) writing to UDP socket. Resetting.",
                        [Reason, inet:format_error(Reason)]),
            {error, Reason}
    end.

-spec send_heartbeat(#state{}) -> 'ok' | {'error', inet:posix() | 'not_owner'}.
send_heartbeat(State) ->
    Heartbeat = molderl_utils:gen_heartbeat(?STATE.stream_name, ?STATE.sequence_number),
    gen_udp:send(?STATE.socket, ?STATE.destination, ?STATE.destination_port, Heartbeat).

%send_endofsession(State) ->
%    EndOfSession = molderl_utils:gen_endofsession(?STATE.stream_name, ?STATE.sequence_number),
%    gen_udp:send(?STATE.socket, ?STATE.destination, ?STATE.destination_port, EndOfSession).

% try to load the disk store of MOLD message blocks
-spec load_store(string()) -> {'ok', non_neg_integer(), [non_neg_integer()]} | {'error', term()}.
load_store(FileName) ->
    case file:open(FileName, [read, raw, binary, read_ahead]) of
        {ok, IoDevice} ->
            Msg = "[molderl] Rebuilding MOLDUDP64 index from disk cache ~p. This may take up to a minute.",
            lager:info(Msg, [FileName]),
            case rebuild_index(IoDevice) of
                {ok, FileSize, Indices} ->
                    % can't pass raw file:io_device() to other processes,
                    % so will need to reopened in molderl_recovery process. 
                    file:close(IoDevice),
                    Fmt = "[molderl] Successfully restored ~p MOLD packets from file ~p",
                    lager:info(Fmt, [length(Indices), FileName]),
                    {ok, FileSize, Indices};
                {error, Reason} ->
                    lager:error("[molderl] Could not restore message store from file ~p because '~p', delete and restart",
                                [FileName, Reason]),
                    {error, Reason}
            end;
        {error, enoent} ->
            lager:info("[molderl] Cannot find message store file ~p, will create new one", [FileName]),
            {ok, 0, []};
        {error, Reason} ->
            lager:error("[molderl] Could not restore message store from file ~p because '~p', delete and restart",
                        [FileName, Reason]),
            {error, Reason}
    end.

% Takes handle to binary file filled with MOLD message blocks and returns a list of indices
% where each MOLD message block starts (in bytes)
-spec rebuild_index(file:io_device()) ->
    {'ok', non_neg_integer(), [non_neg_integer()]} | {'error', term()}.
rebuild_index(IoDevice) ->
    rebuild_index(IoDevice, 0, []).

-spec rebuild_index(file:io_device(), non_neg_integer(), [non_neg_integer()]) ->
    {'ok', non_neg_integer(), [non_neg_integer()]} | {'error', term()}.
rebuild_index(IoDevice, Position, Indices) ->
    case file:pread(IoDevice, Position, 2) of
        {ok, <<Length:16/big-integer>>} ->
            rebuild_index(IoDevice, Position+2+Length, [Position|Indices]);
        eof ->
            {ok, Position, lists:reverse(Indices)};
        {error, Reason} ->
            {error, Reason}
    end.

%% display the content of a disk cache, for debugging purposes. Uncomment if needed.
%-spec cache_representation(file:io_device(), [non_neg_integer()]) -> string().
%cache_representation(IoDevice, Indices) ->
%    cache_representation(IoDevice, Indices, 1, []).
%
%-spec cache_representation(file:io_device(), [non_neg_integer()], pos_integer(), [{pos_integer(),binary()}]) -> string().
%cache_representation(_IoDevice, [], _SeqNum, Cache) ->
%    Strings = [io_lib:format("{~B,~p}", [S,M]) || {S,M} <- lists:reverse(Cache)],
%    lists:concat(['[', string:join(Strings, ","), ']']);
%cache_representation(IoDevice, [Index|Indices], SeqNum, Cache) ->
%    {ok, <<Length:16/big-integer>>} = file:pread(IoDevice, Index, 2),
%    {ok, <<Msg/binary>>} = file:pread(IoDevice, Index+2, Length),
%    cache_representation(IoDevice, Indices, SeqNum+1, [{SeqNum, Msg}|Cache]).

