
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_recovery).

-behaviour(gen_server).

-export([start_link/1, store/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-compile([{parse_transform, lager_transform}]).

-record(state, {
                socket :: port(),                 % Socket to send data on
                stream_name :: binary(),          % Stream name for encoding the response
                last_seq_num :: pos_integer(),    % sequence number of last msg block stored
                packet_size :: pos_integer(),     % maximum packet size of messages in bytes
                blocks_store :: file:io_device(), % file handle to MOLD message blocks store
                store_size :: non_neg_integer(),  % size on disk of msg blocks store
                index :: [integer()],             % indices to MOLD message blocks in above store (reverse order)
                % cache the StatsD keys to prevent repeated atom_to_list/1 calls and concatenation
                statsd_latency_key :: string(),
                statsd_count_key :: string()
               }).

start_link(Arguments) ->
    gen_server:start_link(?MODULE, Arguments, []).

-spec store(pid(), [binary()], [non_neg_integer()], non_neg_integer()) -> ok.
store(Pid, Msgs, MsgsSize, NumMsgs) ->
    gen_server:cast(Pid, {store, Msgs, MsgsSize, NumMsgs}).

init(Arguments) ->

    {streamname, StreamName} = lists:keyfind(streamname, 1, Arguments),
    {recoveryport, RecoveryPort} = lists:keyfind(recoveryport, 1, Arguments),
    {packetsize, PacketSize} = lists:keyfind(packetsize, 1, Arguments),
    {filename, FileName} = lists:keyfind(filename, 1, Arguments),
    {mold_stream, MoldStreamPid} = lists:keyfind(mold_stream, 1, Arguments),

    process_flag(trap_exit, true), % so that terminate/2 gets called when process exits

    self() ! {initialize, FileName, MoldStreamPid},

    {ok, Socket} = gen_udp:open(RecoveryPort, [binary, {active,once}, {reuseaddr, true}]),

    State = #state{socket             = Socket,
                   stream_name        = molderl_utils:gen_streamname(StreamName),
                   packet_size        = PacketSize,
                   statsd_latency_key = "molderl." ++ atom_to_list(StreamName) ++ ".recovery_request.latency",
                   statsd_count_key   = "molderl." ++ atom_to_list(StreamName) ++ ".recovery_request.received"},

    {ok, State}.

handle_cast({store, Msgs, MsgsSize, NumMsgs}, State) ->
    ok = file:write(State#state.blocks_store, Msgs),
    {Positions, NewFileSize} = map_positions(MsgsSize, State#state.store_size),
    {noreply, State#state{index=Positions++State#state.index,
                     last_seq_num=State#state.last_seq_num+NumMsgs,
                     store_size=NewFileSize}}.

handle_call(Msg, _From, State) ->
    lager:warning("[molderl] Unexpected message in module ~p: ~p",[?MODULE, Msg]),
    {noreply, State}.

handle_info({udp, _Client, IP, Port, <<SessionName:10/binary,SequenceNumber:64/big-integer,Count:16/big-integer>>}, State) ->
    TS = os:timestamp(),
    Fmt = "[molderl] Received recovery request from ~p: [session name] ~p [sequence number] ~p [count] ~p",
    lager:debug(Fmt, [IP,string:strip(binary_to_list(SessionName), right), SequenceNumber, Count]),

    % First check recovery request is valid
    case SequenceNumber < 1 orelse SequenceNumber > State#state.last_seq_num of
        true -> % can't request for sequence number bigger what's been stored...
            Fmt2 = "[molderl] invalid recovery request: requested sequence number: ~p, max sequence number: ~p",
            lager:warning(Fmt2, [SequenceNumber, State#state.last_seq_num]);
        false -> % recover msgs from store and send

            % the accounting for the index is a bit hairy since the indices
            % are in reverse order
            Position = lists:nth(State#state.last_seq_num-SequenceNumber+1, State#state.index),
            {ok, Messages} = recover_messages(State#state.blocks_store, Position, Count),

            % Remove messages if bigger than allowed packet size
            {NumMsgs, TruncatedMsgs} = truncate_messages(Messages, State#state.packet_size),
            Payload = molderl_utils:gen_messagepacket(State#state.stream_name, SequenceNumber, NumMsgs, TruncatedMsgs),

            ok = gen_udp:send(State#state.socket, IP, Port, Payload),
            lager:debug("[molderl] Replied recovery request from ~p - reply contains ~p messages", [IP, NumMsgs])
    end,

    statsderl:timing_now(State#state.statsd_latency_key, TS, 0.01),
    statsderl:increment(State#state.statsd_count_key, 1, 0.01),

    ok = inet:setopts(State#state.socket, [{active, once}]),

    {noreply, State};
handle_info({udp, _Client, IP, Port, IllFormedRequest}, State) ->
    Fmt = "[molderl] Received ill-formed recovery request from ~p:~p -> \"~p\".",
    lager:error(Fmt, [IP, Port, IllFormedRequest]),
    ok = inet:setopts(State#state.socket, [{active, once}]),
    {noreply, State};

handle_info({initialize, FileName, MoldStreamPid}, State) ->
    case file:open(FileName, [read, append, raw, binary, read_ahead]) of
        {ok, IoDevice} ->
            Log = "[molderl] Rebuilding MOLDUDP64 index from disk cache ~p. This may take some time.",
            lager:info(Log, [FileName]),
            case rebuild_index(IoDevice) of
                {ok, FileSize, Index} ->
                    SeqNum = length(Index),
                    Fmt = "[molderl] Successfully restored ~p MOLD packets from file ~p",
                    lager:info(Fmt, [SeqNum, FileName]),
                    ok = molderl_stream:set_sequence_number(MoldStreamPid, SeqNum+1),
                    NewState = State#state{last_seq_num=SeqNum,
                                           blocks_store=IoDevice,
                                           store_size=FileSize,
                                           index=Index},
                    {noreply, NewState};
                {error, Reason} ->
                    Msg = "[molderl] Could not restore message store from file ~p because '~p', delete and restart",
                    lager:error(Msg, [FileName, Reason]),
                    {stop, Reason}
            end;
        {error, Reason} ->
            Log = "[molderl] Could not restore message store from file ~p because '~p', delete and restart",
            lager:error(Log, [FileName, Reason]),
            {stop, Reason}
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, State) ->
    Fmt = "[molderl] recovery process for stream ~p is exiting because of reason ~p.",
    lager:info(Fmt, [string:strip(binary_to_list(State#state.stream_name)), Reason]),
    file:sync(State#state.blocks_store),
    file:close(State#state.blocks_store),
    ok = gen_udp:close(State#state.socket).

%% ------------------------------------------------------------
%% Given an io_device(), a position in bytes and a message
%% block count, returns the msg blocks in a list of binaries
%% ------------------------------------------------------------
-spec recover_messages(file:io_device(), non_neg_integer(), non_neg_integer()) ->
    {'ok', [binary()]} | {'error', term()}.
recover_messages(File, Position, Count) ->
    recover_messages(File, Position, Count, []).

-spec recover_messages(file:io_device(), non_neg_integer(), non_neg_integer(), [binary()]) ->
    {'ok', [binary()]} | {'error', term()}.
recover_messages(_File, _Position, 0, MsgBlocks) ->
    {ok, lists:reverse(MsgBlocks)};
recover_messages(File, Position, Count, MsgBlocks) ->
    case file:pread(File, Position, 2) of
        {ok, <<Length:16/big-integer>>} ->
            case file:pread(File, Position+2, Length) of
                {ok, <<Msg/binary>>} ->
                    MsgBlock = <<Length:16/big-integer, Msg/binary>>,
                    recover_messages(File, Position+2+Length, Count-1, [MsgBlock|MsgBlocks]);
                eof ->
                    {error, ill_ended_msg_store};
                {error, Reason} ->
                    {error, Reason}
            end;
        eof ->
            % client asked for more msg blocks than exist,
            % return what is there
            {ok, lists:reverse(MsgBlocks)};
        {error, Reason} ->
            {error, Reason}
    end.

%% ------------------------------------------------------------
%% Given a list of messages sizes (in bytes) and an initial file
%% position (also in bytes), returns a tuple containing (i) the
%% messages sizes each offsetted with the initial file position
%% (in reverse order) and (ii) the ending file position if every
%% message size is added to the initial position
%% ------------------------------------------------------------
-spec map_positions([non_neg_integer()], non_neg_integer()) ->
    {[non_neg_integer()], non_neg_integer()}.
map_positions(MsgsSize, InitialPosition) ->
    map_positions(MsgsSize, InitialPosition, []).

-spec map_positions([non_neg_integer()], non_neg_integer(), [non_neg_integer()]) ->
    {[non_neg_integer()], non_neg_integer()}.
map_positions([], Position, MsgsOffset) ->
    {MsgsOffset, Position};
map_positions([MsgSize|MsgSizes], Position, MsgsOffset) ->
    map_positions(MsgSizes, Position+MsgSize, [Position|MsgsOffset]).

%% ------------------------------------------------------------
%% Takes a list of bitstrings, and returns a truncation of
%% this list which contains just the right number of bitstrings
%% with the right size to be at or under the specified packet
%% size in MOLDUDP64
%% ------------------------------------------------------------
-spec truncate_messages([binary()], pos_integer()) -> {non_neg_integer(), [binary()]}.
truncate_messages(Messages, PacketSize) ->
    truncate_messages(Messages, PacketSize, 20, 0, []).

-spec truncate_messages([binary()], pos_integer(), pos_integer(), non_neg_integer(), [binary()]) ->
    {non_neg_integer(), [binary()]}.
truncate_messages([], _PacketSize, _Size, NumMsgs, Acc) ->
    {NumMsgs, lists:reverse(Acc)};
truncate_messages([Message|Messages], PacketSize, Size, NumMsgs, Acc) ->
    TotalSize = Size+byte_size(Message),
    case TotalSize > PacketSize of
        true ->
            {NumMsgs, lists:reverse(Acc)};
        false ->
            truncate_messages(Messages, PacketSize, TotalSize, NumMsgs+1, [Message|Acc])
    end.

% Takes handle to binary file filled with MOLD message blocks and returns a list of indices
% where each MOLD message block starts (in bytes)
-spec rebuild_index(file:io_device()) ->
    {'ok', non_neg_integer(), [non_neg_integer()]} | {'error', term()}.
rebuild_index(IoDevice) ->
    rebuild_index(IoDevice, <<>>, 0, []).

-spec rebuild_index(file:io_device(), binary(), non_neg_integer(), [non_neg_integer()]) ->
    {'ok', non_neg_integer(), [non_neg_integer()]} | {'error', term()}.
rebuild_index(IoDevice, <<Length:16/big-integer, _Data:Length/binary, Tail/binary>>, Position, Indices) ->
    rebuild_index(IoDevice, Tail, Position+2+Length, [Position|Indices]);
rebuild_index(IoDevice, BinaryBuffer, Position, Indices) ->
    case file:read(IoDevice, 64000) of
        {ok, Data} ->
            rebuild_index(IoDevice, <<BinaryBuffer/binary, Data/binary>>, Position, Indices);
        eof ->
            {ok, Position, Indices};
        {error, Reason} ->
            {error, Reason}
    end.

%%% display the content of a disk cache, for debugging purposes. Uncomment if needed.
%%-spec cache_representation(file:io_device(), [non_neg_integer()]) -> string().
%%cache_representation(IoDevice, Indices) ->
%%    cache_representation(IoDevice, Indices, 1, []).
%%
%%-spec cache_representation(file:io_device(), [non_neg_integer()], pos_integer(), [{pos_integer(),binary()}]) -> string().
%%cache_representation(_IoDevice, [], _SeqNum, Cache) ->
%%    Strings = [io_lib:format("{~B,~p}", [S,M]) || {S,M} <- lists:reverse(Cache)],
%%    lists:concat(['[', string:join(Strings, ","), ']']);
%%cache_representation(IoDevice, [Index|Indices], SeqNum, Cache) ->
%%    {ok, <<Length:16/big-integer>>} = file:pread(IoDevice, Index, 2),
%%    {ok, <<Msg/binary>>} = file:pread(IoDevice, Index+2, Length),
%%    cache_representation(IoDevice, Indices, SeqNum+1, [{SeqNum, Msg}|Cache]).

-ifdef(TEST).

map_positions_test() ->
    ?assertEqual(map_positions([1,2,1,2,1,2], 100), {[107,106,104,103,101,100], 109}).

truncate_messages_test() ->
    Messages = [
        <<>>,
        <<"x">>,
        <<"a","b","c","d","e">>,
        <<"1","2","3">>,
        <<"1","2","3","4","5">>,
        <<"f","o","o","b","a","r","b","a","z">>
    ],
    Packet = truncate_messages(Messages, 33),
    Expected = {4, [<<>>, <<"x">>, <<"a","b","c","d","e">>, <<"1","2","3">>]},
    ?assertEqual(Packet, Expected).

truncate_messages_empty_test() ->
    Packet = truncate_messages([], 40),
    ?assertEqual(Packet, {0, []}).

-endif.

