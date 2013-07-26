-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_recovery).

-define(SERVER, ?MODULE).
-export([init/3]).
-include("molderl.hrl").
-define(STATE,State#state).
 
-record(state, { 
				socket, 		% Socket to send data on
				port,			% Port to send data to
				listen_port,	% Port to listen on
				stream_name,	% Stream name for encoding the response
				ets_name		% ETS table with the replay data in it

				} ).


init(StreamName,Port,ETSName) ->
	{ok, Socket} = gen_udp:open(Port + 1, [binary, {active,true}]),

    State = #state { socket      = Socket,
    				 port        = Port,
    				 listen_port = Port + 1,
    				 stream_name = StreamName,
    				 ets_name    = ETSName
    				},
    loop(State).


loop(State) ->
	receive
		{udp, _Client, IP, _Port, Message} ->
			<<SessionName:10/binary,SequenceNumber:64/big-integer,Count:16/big-integer>> = Message,
			io:format("received recovery request from ~p: [session name] ~p  [sequence number] ~p  [count] ~p", [IP,SessionName,SequenceNumber,Count]),
			% Get messages from recovery table
			% Generated with ets:fun2ms(fun({X,Y}) when X < Min + Count ,X > 2 -> Y end).
			Messages = ets:select(recovery_table,[{{'$1','$2'},[{'=<','$1',SequenceNumber + Count -1},{'>=','$1',SequenceNumber}],['$2']}]),
			% Generate a MOLD packet
			EncodedMessage = molderl_utils:gen_messagepacket_without_seqnum(?STATE.stream_name,SequenceNumber,Messages),
			% Send that packet back
			gen_udp:send(?STATE.socket,IP,?STATE.port,EncodedMessage),
			% Loop - and we're done
			loop(State)
	end.
