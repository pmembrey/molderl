-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_stream).
-export([init/6]).
-include("molderl.hrl").

-define(PACKET_SIZE,1200).
-define(STATE,State#state).

-record(state, { stream_name, destination,sequence_number, socket, destination_port, stream_process_name,messages, message_length,timer,timer_ref } ).


init(StreamProcessName,StreamName,Destination,DestinationPort,IPAddressToSendFrom,Timer) ->
    register(StreamProcessName,self()),
    {ok, Socket} = gen_udp:open( 0, [binary, {broadcast, true},{ip, IPAddressToSendFrom}]),
    MoldStreamName = molderl_utils:gen_streamname(StreamName),
    % Create ETS table to store recovery stream (currently unlimited right now)
    ets:new(recovery_table,[ordered_set,named_table]),
    % Kick off the timer, keep the reference (TRef) so we can cancel it if we send before the timer is hit
    {ok,TRef} = timer:send_after(Timer,send_from_timer),
    State = #state{     stream_name = MoldStreamName,                     % Name of the stream encoded for MOLD64 (i.e. padded binary)
                        destination = Destination,                        % The IP address to send / broadcast / multicast to
                        sequence_number = 1,                              % Next sequence number
                        socket = Socket,                                  % The socket to send the data on
                        destination_port = DestinationPort,               % Destination port for the data
                        stream_process_name = StreamProcessName,          % The Erlang process name (it's an atom)
                        messages = [],                                    % List of messages waiting to be encoded aznd sent
                        message_length = 0,                               % Current length of messages if they were to be encoded in a MOLD64 packet
                        timer = Timer,                                    % Timer for the auto-send. Ensures data never sits pending for too long
                        timer_ref = TRef                                  % Reference to said timer to allow it to be canceled if message has just been sent
                                                                          % i.e. when a send was triggered by a full packet
                  },
    loop(State).


loop(State) ->
    receive
      {send,Message} -> % Time to send a message!
          % Calculate message length
          MessageLength = message_length(?STATE.message_length,Message),
          % Can we fit this in?
          case MessageLength > ?PACKET_SIZE of
            true    ->    % Nope we can't, send what we have and requeue
                          % Cancel timer
                          timer:cancel(?STATE.timer_ref),
                          {NextSequence,EncodedMessage,MessagesWithSequenceNumbers} = molderl_utils:gen_messagepacket(?STATE.stream_name,?STATE.sequence_number,?STATE.messages),
                          % Send message
                          send_message(State,EncodedMessage),
                          % Insert into recovery table
                          ets:insert(recovery_table,MessagesWithSequenceNumbers),

                          % Schedule a new timer
                          {ok,TRef} = timer:send_after(?STATE.timer,send_from_timer),
                          % Loop
                          loop(?STATE{message_length = message_length(0,Message),messages = [Message],sequence_number = NextSequence, timer_ref = TRef});
            false   ->    % Yes we can - add it to the list of messages
                          loop(?STATE{message_length = MessageLength,messages = ?STATE.messages ++ [Message]})
          end;
      send_from_timer ->    % Timer triggered a send
                              case length(?STATE.messages) > 0 of
                                true ->
                                  {NextSequence,EncodedMessage,MessagesWithSequenceNumbers} = molderl_utils:gen_messagepacket(?STATE.stream_name,?STATE.sequence_number,?STATE.messages),
                                  % Send message
                                  send_message(State,EncodedMessage),
                                  % Reset timer
                                  timer:send_after(?STATE.timer,send_from_timer),
                                  % Insert into recovery table
                                  ets:insert(recovery_table,MessagesWithSequenceNumbers),
                                  loop(?STATE{message_length = 0,messages = [],sequence_number = NextSequence});
                                false ->
                                  timer:send_after(?STATE.timer,send_from_timer),
                                  loop(?STATE{message_length = 0,messages = []})
                                end

    after 1000 -> 
      send_heartbeat(State),
      loop(State)
    end.




send_message(State,EncodedMessage) ->
    gen_udp:send(?STATE.socket,?STATE.destination, ?STATE.destination_port, EncodedMessage).

send_heartbeat(State) ->
    Heartbeat = molderl_utils:gen_heartbeat(?STATE.stream_name,?STATE.sequence_number),
    gen_udp:send(?STATE.socket,?STATE.destination, ?STATE.destination_port, Heartbeat).

send_endofsession(State) ->
    EndOfSession = molderl_utils:gen_endofsession(?STATE.stream_name,?STATE.sequence_number),
    gen_udp:send(?STATE.socket,?STATE.destination, ?STATE.destination_port, EndOfSession).


message_length(0,Message) ->
    % Header is 20 bytes
    % 2 bytes for length of message
    % X bytes for message
    22 + byte_size(Message);
message_length(Size,Message) ->
    % Need to add 2 bytes for the length
    % and X bytes for the message itself
    Size + 2 + byte_size(Message).