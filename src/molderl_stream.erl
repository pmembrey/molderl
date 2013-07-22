-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_stream).
-export([init/4]).
-include("molderl.hrl").

-define(PACKET_SIZE,1200).
-define(STATE,State#state).

-record(state, { stream_name, destination,sequence_number, socket, destination_port, stream_process_name,messages, message_length,timer,timer_ref } ).

init(StreamProcessName,StreamName,Destination,DestinationPort) ->
    register(StreamProcessName,self()),
    {ok, Socket} = gen_udp:open( 0, [binary, {broadcast, true}]),
    MoldStreamName = molderl_utils:gen_streamname(StreamName),
    % Kick off the timer, keep the reference (TRef) so we can cancel it if we send before the timer is hit
    {ok,TRef} = timer:send_after(5000,send_from_timer),
    State = #state{stream_name = MoldStreamName, destination = Destination,sequence_number = 1,socket = Socket,destination_port = DestinationPort, stream_process_name = StreamProcessName, messages = [], message_length = 0, timer = 5000,timer_ref = TRef},
    loop(State).


loop(State) ->
    receive
      {send,Message} -> % Time to send a message!
          % Calculate message length
          MessageLength = message_length(?STATE.message_length,Message),
          % Can we fit this in?
          case MessageLength > ?PACKET_SIZE of
            true    ->    % Nope we can't, send what we have and requeue
                          {NextSequence,EncodedMessage} = molderl_utils:gen_messagepacket(?STATE.stream_name,?STATE.sequence_number,?STATE.messages),
                          % Send message
                          send_message(State,EncodedMessage),
                          % Cancel timer
                          timer:cancel(?STATE.timer_ref),
                          % Schedule a new timer
                          timer:send_after(?STATE.timer,send_from_timer),
                          % Loop
                          loop(?STATE{message_length = message_length(0,Message),messages = [Message],sequence_number = NextSequence});
            false   ->    % Yes we can - add it to the list of messages
                          loop(?STATE{message_length = MessageLength,messages = ?STATE.messages ++ [Message]})
          end;
      send_from_timer ->    % Timer triggered a send
                              case length(?STATE.messages) > 0 of
                                true ->
                                  {NextSequence,EncodedMessage} = molderl_utils:gen_messagepacket(?STATE.stream_name,?STATE.sequence_number,?STATE.messages),
                                  % Send message
                                  send_message(State,EncodedMessage),
                                  % Reset timer
                                  timer:send_after(?STATE.timer,send_from_timer),
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
    gen_udp:send(?STATE.socket,{255,255,255,255}, ?STATE.destination_port, EncodedMessage).

send_heartbeat(State) ->
    Heartbeat = molderl_utils:gen_heartbeat(?STATE.stream_name,?STATE.sequence_number),
    gen_udp:send(?STATE.socket,{255,255,255,255}, ?STATE.destination_port, Heartbeat).

send_endofsession(State) ->
    EndOfSession = molderl_utils:gen_endofsession(?STATE.stream_name,?STATE.sequence_number),
    gen_udp:send(?STATE.socket,{255,255,255,255}, ?STATE.destination_port, EndOfSession).


message_length(0,Message) ->
    % Header is 20 bytes
    % 2 bytes for length of message
    % X bytes for message
    22 + byte_size(Message);
message_length(Size,Message) ->
    % Need to add 2 bytes for the length
    % and X bytes for the message itself
    Size + 2 + byte_size(Message).