
-module(molderl_stream).
-export([init/6]).

-define(PACKET_SIZE,1200).
-define(STATE,State#state).

-record(state, { stream_name,           % Name of the stream encoded for MOLD64 (i.e. padded binary)
                 destination,           % The IP address to send / broadcast / multicast to
                 sequence_number = 1,   % Next sequence number
                 socket,                % The socket to send the data on
                 destination_port,      % Destination port for the data
                 stream_process_name,   % The Erlang process name (it's an atom)
                 messages = [],         % List of messages waiting to be encoded and sent
                 message_length = 0,    % Current length of messages if they were to be encoded in a MOLD64 packet
                 recovery_process       % Process that handles dropped packet recovery
               } ).


init(StreamProcessName,StreamName,Destination,DestinationPort,IPAddressToSendFrom,ProdInterval) ->
    register(StreamProcessName,self()),
    {ok, Socket} = gen_udp:open( 0, [binary,
                                     {broadcast, true},
                                     {ip, IPAddressToSendFrom},
                                     {add_membership, {Destination, IPAddressToSendFrom}},
                                     {multicast_if, IPAddressToSendFrom}]),
    MoldStreamName = molderl_utils:gen_streamname(StreamName),
    % Create ETS table to store recovery stream (currently unlimited right now)
    ets:new(recovery_table,[ordered_set,named_table]),
    % Kick off the Prodding process...
    spawn_link(molderl_prodder,init,[self(),ProdInterval]),
    RecoveryProcess = spawn_link(molderl_recovery,init,[MoldStreamName,DestinationPort,recovery_table, ?PACKET_SIZE]),
    State = #state{stream_name = MoldStreamName,
                   destination = Destination,
                   socket = Socket,
                   destination_port = DestinationPort,
                   stream_process_name = StreamProcessName,
                   recovery_process = RecoveryProcess
                  },
    loop(State).

loop(State) ->
    receive
        {send,Message} ->
            MessageLength = molderl_utils:message_length(?STATE.message_length,Message),
            % Can we fit this in?
            case MessageLength > ?PACKET_SIZE of
                true -> % Nope we can't, send what we have and requeue
                    MsgPkt = molderl_utils:gen_messagepacket(?STATE.stream_name,
                                                             ?STATE.sequence_number,
                                                             lists:reverse(?STATE.messages)),
                    {NextSequence, EncodedMessage, MessagesWithSequenceNumbers} = MsgPkt,
                    send_message(State, EncodedMessage),
                    ets:insert(recovery_table, MessagesWithSequenceNumbers),
                    loop(?STATE{message_length = molderl_utils:message_length(0,Message),
                                messages = [Message],
                                sequence_number = NextSequence});
                false -> % Yes we can - add it to the list of messages
                    loop(?STATE{message_length = MessageLength, messages = [Message|?STATE.messages]})
            end;
        prod -> % Timer triggered a send
            case ?STATE.messages of
                [] ->
                    send_heartbeat(State),
                    loop(?STATE{message_length = 0,messages = []});
                _ ->
                    MsgPkt = molderl_utils:gen_messagepacket(?STATE.stream_name,
                                                             ?STATE.sequence_number,
                                                             lists:reverse(?STATE.messages)),
                    {NextSequence, EncodedMessage, MessagesWithSequenceNumbers} = MsgPkt,
                    send_message(State,EncodedMessage),
                    ets:insert(recovery_table,MessagesWithSequenceNumbers),
                    loop(?STATE{message_length = 0, messages = [], sequence_number = NextSequence})
            end

    after 1000 ->
        send_heartbeat(State),
        loop(State)
    end.

send_message(State, EncodedMessage) ->
    gen_udp:send(?STATE.socket, ?STATE.destination, ?STATE.destination_port, EncodedMessage).

send_heartbeat(State) ->
    Heartbeat = molderl_utils:gen_heartbeat(?STATE.stream_name, ?STATE.sequence_number),
    gen_udp:send(?STATE.socket, ?STATE.destination, ?STATE.destination_port, Heartbeat).

%send_endofsession(State) ->
%    EndOfSession = molderl_utils:gen_endofsession(?STATE.stream_name, ?STATE.sequence_number),
%    gen_udp:send(?STATE.socket, ?STATE.destination, ?STATE.destination_port, EndOfSession).

