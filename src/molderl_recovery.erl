
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_recovery).

-behaviour(gen_server).

-export([start_link/3, store/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(STATE,State#state).

-record(state, { 
                socket,           % Socket to send data on
                stream_name,      % Stream name for encoding the response
                table_id,         % ETS table with the replay data in it
                packet_size       % maximum packet size of messages in bytes
               }).

start_link(StreamName, RecoveryPort, PacketSize) ->
    gen_server:start_link(?MODULE, [StreamName, RecoveryPort, PacketSize], []).

store(Pid, Item) ->
    gen_server:cast(Pid, {store, Item}).

init([StreamName, RecoveryPort, PacketSize]) ->

    {ok, Socket} = gen_udp:open(RecoveryPort, [binary, {active,true}]),

    State = #state {
                    socket           = Socket,
                    stream_name      = StreamName,
                    table_id         = ets:new(recovery_table, [ordered_set]),
                    packet_size      = PacketSize
                   },
    {ok, State}.

handle_cast({store, Item}, State) ->
    ets:insert(?STATE.table_id, Item),
    {noreply, State}.

handle_info({udp, _Client, IP, Port, Message}, State) ->
    <<SessionName:10/binary,SequenceNumber:64/big-integer,Count:16/big-integer>> = Message,
    io:format("Received recovery request from ~p: [session name] ~p [sequence number] ~p [count] ~p~n",
              [IP,SessionName,SequenceNumber,Count]),
    % Get messages from recovery table
    % Generated with ets:fun2ms(fun({X,Y}) when X < Min + Count ,X > 2 -> Y end).
    Messages = ets:select(?STATE.table_id,
                          [{{'$1','$2'},[{'=<','$1',SequenceNumber + Count -1},{'>=','$1',SequenceNumber}],['$2']}]),
    % Remove messages if bigger than allowed packet size
    TruncatedMessages = truncate_messages(Messages, ?STATE.packet_size),
    % Generate a MOLD packet
    EncodedMessage = molderl_utils:gen_messagepacket_without_seqnum(?STATE.stream_name,SequenceNumber,TruncatedMessages),
    gen_udp:send(?STATE.socket,IP,Port,EncodedMessage),
    {noreply, State}.

handle_call(Msg, _From, State) ->
    io:format("Unexpected message in module ~p: ~p~n",[?MODULE, Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(normal, _State) ->
    ok.

%% ------------------------------------------------------------
%% Takes a list of bitstrings, and returns a truncation of
%% this list which contains just the right number of bitstrings
%% with the right size to be at or under the specified packet
%% size in Mold 64
%% ------------------------------------------------------------
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

