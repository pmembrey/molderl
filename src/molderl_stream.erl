-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_stream).
-export([init/4]).
-include("molderl.hrl").

-define(STATE,State#state).

-record(state, { stream_name, destination,sequence_number, socket, destination_port, stream_process_name } ).

init(StreamProcessName,StreamName,Destination,DestinationPort) ->
    register(StreamProcessName,self()),
    {ok, Socket} = gen_udp:open( 0, [binary, {broadcast, true}]),
    MoldStreamName = molderl_utils:gen_streamname(StreamName),
    State = #state{stream_name = MoldStreamName, destination = Destination,sequence_number = 1,socket = Socket,destination_port = DestinationPort, stream_process_name = StreamProcessName},
    loop(State).


loop(State) ->
    receive
      {send,Message} -> % Time to send a message!
          % Encode the message and get the next sequence number
          {NextSequence,EncodedMessage} = molderl_utils:gen_messagepacket(?STATE.stream_name,?STATE.sequence_number,Message),
          % Send the message
          send_message(State,EncodedMessage),
          % Loop
          loop(?STATE{sequence_number = NextSequence})
    after 1000 -> 
      send_heartbeat(State),
      loop(State)
    end.




send_message(State,EncodedMessage) ->
    gen_udp:send(?STATE.socket,{255,255,255,255}, ?STATE.destination_port, EncodedMessage).

send_heartbeat(State) ->
    Heartbeat = molderl_utils:gen_heartbeat(?STATE.stream_name,?STATE.sequence_number),
    gen_udp:send(?STATE.socket,{255,255,255,255}, ?STATE.destination_port, Heartbeat).