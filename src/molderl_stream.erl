
-module(molderl_stream).

-behaviour(gen_server).

-export([start_link/7, prod/1, send/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("molderl.hrl").

-define(STATE,State#state).

-record(state, { stream_name,           % Name of the stream encoded for MOLD64 (i.e. padded binary)
                 destination,           % The IP address to send / broadcast / multicast to
                 sequence_number = 1,   % Next sequence number
                 socket,                % The socket to send the data on
                 destination_port,      % Destination port for the data
                 messages = [],         % List of messages waiting to be encoded and sent
                 message_length = 0,    % Current length of messages if they were to be encoded in a MOLD64 packet
                 recovery_service       % Pid of the recovery service message
               }).

start_link(SupervisorPid, StreamName, Destination, DestinationPort,
           RecoveryPort, IPAddressToSendFrom, Timer) ->
    gen_server:start_link({local, StreamName},
                          ?MODULE,
                          [SupervisorPid, StreamName, Destination, DestinationPort,
                           RecoveryPort, IPAddressToSendFrom, Timer],
                          []).

send(Pid, Message) ->
    gen_server:cast(Pid, {send, Message}).

prod(Pid) ->
    gen_server:cast(Pid, prod).

init([SupervisorPID, StreamName, Destination, DestinationPort,
      RecoveryPort, IPAddressToSendFrom, ProdInterval]) ->

    MoldStreamName = molderl_utils:gen_streamname(StreamName),

    % send yourself a reminder to start recovery & prodder
    self() ! {initialize, SupervisorPID, MoldStreamName, RecoveryPort, ?PACKET_SIZE, ProdInterval},

    Connection = gen_udp:open(0, [binary,
                                    {broadcast, true},
                                    {ip, IPAddressToSendFrom},
                                    {add_membership, {Destination, IPAddressToSendFrom}},
                                    {multicast_if, IPAddressToSendFrom}]),

    case Connection of
        {ok, Socket} ->
            State = #state{stream_name = MoldStreamName,
                           destination = Destination,
                           socket = Socket,
                           destination_port = DestinationPort
                          },
            {ok, State, 1000}; % third element is timeout
        {error, Reason} ->
            io:format("Unable to open UDP socket on ~p because ~p. Aborting.~n",
                      [IPAddressToSendFrom, inet:format_error(Reason)]),
            {stop, Reason}
    end.

handle_cast({send, Message}, State) ->
    MessageLength = molderl_utils:message_length(?STATE.message_length,Message),
    % Can we fit this in?
    case MessageLength > ?PACKET_SIZE of
        true -> % Nope we can't, send what we have and requeue
            MsgPkt = molderl_utils:gen_messagepacket(?STATE.stream_name,
                                                     ?STATE.sequence_number,
                                                     lists:reverse(?STATE.messages)),
            {NextSequence, EncodedMessage, MessagesWithSequenceNumbers} = MsgPkt,
            send_message(State, EncodedMessage),
            molderl_recovery:store(?STATE.recovery_service, MessagesWithSequenceNumbers),
            {noreply, ?STATE{message_length = molderl_utils:message_length(0,Message),
                             messages = [Message],
                             sequence_number = NextSequence}};
        false -> % Yes we can - add it to the list of messages
            {noreply, ?STATE{message_length = MessageLength, messages = [Message|?STATE.messages]}}
    end;
handle_cast(prod, State) -> % Timer triggered a send
    case ?STATE.messages of
        [] ->
            send_heartbeat(State),
            {noreply, ?STATE{message_length = 0, messages = []}};
        _ ->
            MsgPkt = molderl_utils:gen_messagepacket(?STATE.stream_name,
                                                     ?STATE.sequence_number,
                                                     lists:reverse(?STATE.messages)),
            {NextSequence, EncodedMessage, MessagesWithSequenceNumbers} = MsgPkt,
            send_message(State,EncodedMessage),
            molderl_recovery:store(?STATE.recovery_service, MessagesWithSequenceNumbers),
            {noreply, ?STATE{message_length = 0, messages = [], sequence_number = NextSequence}}
    end.

handle_info(timeout, State) ->
    send_heartbeat(State),
    {noreply, State};
handle_info({initialize, SupervisorPID, MoldStreamName, RecoveryPort, PacketSize, ProdInterval}, State) ->
    ProdderSpec = ?CHILD(make_ref(), molderl_prodder, [self(), ProdInterval], transient, worker),
    supervisor:start_child(SupervisorPID, ProdderSpec),
    RecoverySpec = ?CHILD(make_ref(), molderl_recovery, [MoldStreamName, RecoveryPort, PacketSize], transient, worker),
    {ok, RecoveryProcess} = supervisor:start_child(SupervisorPID, RecoverySpec),
    {noreply, ?STATE{recovery_service=RecoveryProcess}}.

handle_call(Msg, _From, State) ->
    io:format("Unexpected message in module ~p: ~p~n",[?MODULE, Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(normal, _State) ->
    ok.

send_message(State, EncodedMessage) ->
    gen_udp:send(?STATE.socket, ?STATE.destination, ?STATE.destination_port, EncodedMessage).

send_heartbeat(State) ->
    Heartbeat = molderl_utils:gen_heartbeat(?STATE.stream_name, ?STATE.sequence_number),
    gen_udp:send(?STATE.socket, ?STATE.destination, ?STATE.destination_port, Heartbeat).

%send_endofsession(State) ->
%    EndOfSession = molderl_utils:gen_endofsession(?STATE.stream_name, ?STATE.sequence_number),
%    gen_udp:send(?STATE.socket, ?STATE.destination, ?STATE.destination_port, EndOfSession).

