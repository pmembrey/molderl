
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_recovery).

-behaviour(gen_server).

-export([start_link/3, store/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-compile([{parse_transform, lager_transform}]).

-define(STATE,State#state).

-record(state, {
                socket :: port(),             % Socket to send data on
                stream_name,                  % Stream name for encoding the response
                packet_size :: pos_integer(), % maximum packet size of messages in bytes
                cache = [] :: list(),         % list of MOLD messages to recover from
                % cache number of messages in and size of the cache,
                % faster than calling length(cache) and byte_size(cache) everytime
                cache_size_msgs = 0 :: non_neg_integer(),
                cache_size_bytes = 0 :: non_neg_integer(),
                % cache the StatsD keys to prevent repeated atom_to_list/1 calls and concatenation
                statsd_latency_key :: string(),
                statsd_count_key :: string(),
                statsd_memory_key :: string(),
                statsd_msgs_key :: string()
               }).

start_link(StreamName, RecoveryPort, PacketSize) ->
    gen_server:start_link(?MODULE, [StreamName, RecoveryPort, PacketSize], []).

-spec store(pid(), pos_integer(), pos_integer(), [binary()]) -> ok.
store(Pid, NumMsgs, NumBytes, Msgs) ->
    gen_server:cast(Pid, {store, Msgs, NumMsgs, NumBytes}).

init([StreamName, RecoveryPort, PacketSize]) ->

    process_flag(trap_exit, true), % so that terminate/2 gets called when process exits

    {ok, Socket} = gen_udp:open(RecoveryPort, [binary, {active,once}]),

    State = #state {
                    socket             = Socket,
                    stream_name        = molderl_utils:gen_streamname(StreamName),
                    packet_size        = PacketSize,
                    statsd_latency_key = "molderl." ++ atom_to_list(StreamName) ++ ".recovery_request.latency",
                    statsd_count_key   = "molderl." ++ atom_to_list(StreamName) ++ ".recovery_request.received",
                    statsd_memory_key  = "molderl." ++ atom_to_list(StreamName) ++ ".bytes.cached",
                    statsd_msgs_key  = "molderl." ++ atom_to_list(StreamName) ++ ".messages.cached"
                   },
    {ok, State}.

handle_cast({store, Msgs, NumMsgs, NumBytes}, State) ->
    statsderl:gauge(?STATE.statsd_msgs_key, ?STATE.cache_size_msgs+NumMsgs, 0.01),
    statsderl:gauge(?STATE.statsd_memory_key, ?STATE.cache_size_bytes+NumBytes, 0.01),
    {noreply, ?STATE{cache=Msgs++?STATE.cache,
                     cache_size_msgs=?STATE.cache_size_msgs+NumMsgs,
                     cache_size_bytes=?STATE.cache_size_bytes+NumBytes}}.

handle_info({udp, _Client, IP, Port, <<SessionName:10/binary,SequenceNumber:64/big-integer,Count:16/big-integer>>}, State) ->
    TS = os:timestamp(),
    Fmt = "[molderl] Received recovery request from ~p: [session name] ~p [sequence number] ~p [count] ~p",
    lager:debug(Fmt, [IP,string:strip(binary_to_list(SessionName), right),SequenceNumber,Count]),

    % First sanitize input
    case SequenceNumber > ?STATE.cache_size_msgs of
        true -> % can't request for sequence number bigger than cache size...
            Fmt2 = "[molderl] received incorrect recovery request - sequence number: ~p, cache size: ~p",
            lager:warning(Fmt2, [SequenceNumber, ?STATE.cache_size_msgs]);
        false -> % recover msgs from cache and send

            % The math to infer indices is a bit tricky because the cache is in reverse order
            Start = max(?STATE.cache_size_msgs-SequenceNumber-Count+2, 1),
            Len = min(?STATE.cache_size_msgs-SequenceNumber+1, Count),
            Messages = lists:sublist(?STATE.cache, Start, Len),

            % Remove messages if bigger than allowed packet size
            TruncatedMsgs = truncate_messages(Messages, ?STATE.packet_size),
            {EncodedMsgs, NumMsgs, _NumBytes} = molderl_utils:encode_messages(TruncatedMsgs),
            Payload = molderl_utils:gen_messagepacket(?STATE.stream_name, SequenceNumber, NumMsgs, EncodedMsgs),

            ok = gen_udp:send(?STATE.socket, IP, Port, Payload)
    end,

    statsderl:timing_now(?STATE.statsd_latency_key, TS, 0.01),
    statsderl:increment(?STATE.statsd_count_key, 1, 0.01),

    ok = inet:setopts(?STATE.socket, [{active, once}]),

    {noreply, State};
handle_info({udp, _Client, IP, Port, IllFormedRequest}, State) ->
    Fmt = "[molderl] Received ill-formed recovery request from ~p:~p -> \"~p\".",
    lager:error(Fmt, [IP, Port, IllFormedRequest]),
    ok = inet:setopts(?STATE.socket, [{active, once}]),
    {noreply, State}.

handle_call(Msg, _From, State) ->
    lager:warning("[molderl] Unexpected message in module ~p: ~p",[?MODULE, Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, State) ->
    Fmt = "[molderl] recovery process for stream ~p is exiting because of reason ~p.",
    lager:error(Fmt, [string:strip(binary_to_list(State#state.stream_name)), Reason]),
    ok = gen_udp:close(State#state.socket).

%% ------------------------------------------------------------
%% Takes a list of bitstrings, and returns a truncation of
%% this list which contains just the right number of bitstrings
%% with the right size to be at or under the specified packet
%% size in Mold 64
%% ------------------------------------------------------------
-spec truncate_messages([binary()], pos_integer()) -> [binary()].
truncate_messages(Messages, PacketSize) ->
    truncate_messages(Messages, PacketSize, 0, []).

-spec truncate_messages([binary()], pos_integer(), non_neg_integer(), [binary()]) -> [binary()].
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
    Expected = [<<>>, <<"x">>, <<"a","b","c","d","e">>, <<"1","2","3">>],
    ?assertEqual(Packet, Expected).

truncate_messages_empty_test() ->
    Packet = truncate_messages([], 40),
    ?assertEqual(Packet, []).

-endif.

