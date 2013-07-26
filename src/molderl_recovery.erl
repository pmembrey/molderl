-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_recovery).

-define(SERVER, ?MODULE).
-export([init/3]).
-include("molderl.hrl").
 
-record(state, { 
				socket, 		% Socket to send data on
				port,			% Port to send data to
				stream_name,	% Stream name for encoding the response
				ets_name		% ETS table with the replay data in it

				} ).


init(StreamName,Port,ETSName) ->
	{ok, Socket} = gen_udp:open(Port, [binary, {active,true}]),

    State = #state { socket      = Socket,
    				 port        = Port,
    				 stream_name = StreamName,
    				 ets_name    = ETSName
    				},
    loop(State).


loop(State) ->
	receive
		{udp, _Client, IP, _Port, Message} ->
			<<SessionName:10/binary,SequenceNumber:64/big-integer,Count:16/big-integer>> = Message,
			io:format("received recovery request from ~p: [session name] ~p  [sequence number] ~p  [count] ~p", [IP,SessionName,SequenceNumber,Count]),
			loop(State)
	end.
