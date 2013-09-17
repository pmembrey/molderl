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
				ets_name,		% ETS table with the replay data in it
                packet_size     % maximum packet size of messages in bytes
				} ).


init(StreamName,Port,ETSName,PacketSize) ->
	{ok, Socket} = gen_udp:open(Port + 1, [binary, {active,true}]),

    State = #state { socket      = Socket,
    				 port        = Port,
    				 listen_port = Port + 1,
    				 stream_name = StreamName,
    				 ets_name    = ETSName,
                     packet_size = PacketSize
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
            % Remove messages if bigger than allowed packet size
            TruncatedMessages = truncate_messages(Messages, ?STATE.packet_size),
            % Generate a MOLD packet
            EncodedMessage = molderl_utils:gen_messagepacket_without_seqnum(?STATE.stream_name,SequenceNumber,TruncatedMessages),
            % Send that packet back
            gen_udp:send(?STATE.socket,IP,?STATE.port,EncodedMessage);
            % Loop - and we're done
        Other ->
            io:format("function ~p:loop/1 received unexpected message: ~p~n", [?MODULE, Other])
    end,
    loop(State).

%% ------------------------------
%% Takes a list of bitstrings,
%% and returns a truncation of
%% this list which contains just
%% the right number of bitstrings
%% with the right size to be at
%% or under the specified packet
%% size in Mold 64
%% ------------------------------
truncate_messages(Messages, PacketSize) ->
    truncate_messages(Messages, PacketSize, 0, []).

truncate_messages([], _PacketSize, _Size, Acc) ->
    lists:reverse(Acc);
truncate_messages([Message|Messages], PacketSize, Size, Acc) ->
    MessageLen = molderl_utils:message_length(Size, Message),
    case MessageLen > PacketSize of
        true ->
            lists:reverse(Acc);
        false ->
            truncate_messages(Messages, PacketSize, MessageLen, [Message|Acc])
    end.

-ifdef(TEST).

truncate_messages_test() ->
    Messages = [
                    <<>>,
                    <<"x">>,
                    <<"a","b","c","d","e">>,
                    <<"1","2","3">>,
                    <<"1","2","3","4","5">>,
                    <<"f","o","o","b","a","r","b","a","z">>
               ],
    Packet = truncate_messages(Messages, 40),
    Expected = [<<>>,<<"x">>,<<"a","b","c","d","e">>,<<"1","2","3">>],
    ?assertEqual(Packet, Expected).
    
-endif.

