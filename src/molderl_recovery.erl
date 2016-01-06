
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
                socket :: port(),                    % Socket to send data on
                stream_name :: binary(),             % Stream name for encoding the response
                last_seq_num :: pos_integer(),       % sequence number of last msg block stored
                packet_size :: pos_integer(),        % maximum packet size of messages in bytes
                blocks_store :: file:io_device(),    % file handle to MOLD message blocks store
                store_size :: non_neg_integer(),     % size on disk of msg blocks store
                index :: [integer()],                % indices to MOLD message blocks in above store (reverse order)
                max_recovery_count :: pos_integer(), % ignore recovery requests with counts above this
                % cache the StatsD keys to prevent repeated atom_to_list/1 calls and concatenation
                statsd_latency_key :: string(),
                statsd_count_key :: string()
               }).

start_link(Arguments) ->
    gen_server:start_link(?MODULE, Arguments, []).

-spec store(atom(), [binary()], [non_neg_integer()], non_neg_integer()) -> ok.
store(ProcessName, Msgs, MsgsSize, NumMsgs) ->
    gen_server:cast(ProcessName, {store, Msgs, MsgsSize, NumMsgs}).

init(Arguments) ->

    {streamname, StreamName} = lists:keyfind(streamname, 1, Arguments),
    {recoveryprocessname, RecoveryProcessName} = lists:keyfind(recoveryprocessname, 1, Arguments),
    {recoveryport, RecoveryPort} = lists:keyfind(recoveryport, 1, Arguments),
    {packetsize, PacketSize} = lists:keyfind(packetsize, 1, Arguments),
    {filename, FileName} = lists:keyfind(filename, 1, Arguments),
    {mold_stream, MoldStreamProcessName} = lists:keyfind(mold_stream, 1, Arguments),
    {max_recovery_count, MaxRecoveryCount} = lists:keyfind(max_recovery_count, 1, Arguments),

    process_flag(trap_exit, true), % so that terminate/2 gets called when process exits

    self() ! {initialize, FileName, MoldStreamProcessName},

    {ok, Socket} = gen_udp:open(RecoveryPort, [binary, {active,once}, {reuseaddr, true}]),

    State = #state{socket             = Socket,
                   stream_name        = molderl_utils:gen_streamname(StreamName),
                   packet_size        = PacketSize,
                   max_recovery_count = MaxRecoveryCount,
                   statsd_latency_key = "molderl." ++ atom_to_list(StreamName) ++ ".recovery_request.latency",
                   statsd_count_key   = "molderl." ++ atom_to_list(StreamName) ++ ".recovery_request.received"},

    register(RecoveryProcessName, self()),
    lager:info("[molderl] Register molderl_recovery pid[~p] with name[~p]", [self(), RecoveryProcessName]),
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
    case validate_request(SequenceNumber, Count, State#state.last_seq_num, State#state.max_recovery_count) of
        ok -> % recover msgs from store and send

            % the accounting for the index is a bit hairy since the indices
            % are in reverse order
            Position = lists:nth(State#state.last_seq_num-SequenceNumber+1, State#state.index),
            {ok, NumMsgs, Messages} = recover_messages(State#state.blocks_store,Position,Count,State#state.packet_size,20,0,[]),

            Payload = molderl_utils:gen_messagepacket(State#state.stream_name, SequenceNumber, NumMsgs, Messages),

            ok = gen_udp:send(State#state.socket, IP, Port, Payload),
            lager:debug("[molderl] Replied recovery request from ~p - reply contains ~p messages", [IP, NumMsgs]);

        {warning, Reason} ->
            lager:warning("[molderl] Unable to service recovery request from ~p because ~s. Ignoring.", [IP, Reason]);

        {error, Reason} ->
            lager:error("[molderl] Unable to service recovery request from ~p because ~s. Ignoring.", [IP, Reason])
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

handle_info({initialize, FileName, MoldStreamProcessName}, State) ->
    case file:open(FileName, [read, append, raw, binary, read_ahead]) of
        {ok, IoDevice} ->
            Log = "[molderl] Rebuilding MOLDUDP64 index from disk cache ~p. This may take some time.",
            lager:info(Log, [FileName]),
            case rebuild_index(IoDevice) of
                {ok, FileSize, Index} ->
                    SeqNum = length(Index),
                    Fmt = "[molderl] Successfully restored ~p MOLD packets from file ~p",
                    lager:info(Fmt, [SeqNum, FileName]),
                    ok = molderl_stream:set_sequence_number(MoldStreamProcessName, SeqNum+1),
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

%% -------------------------------------------------------------
%% Validate a recovery request. Check that the requested
%% sequence number is bigger than zero but smaller than the
%% biggest sequence number that was sent out. Check that the
%% request count is smaller than the maximum request count.
%% ------------------------------------------------------------
-spec validate_request(integer(), integer(), pos_integer(), pos_integer()) ->
    'ok' | {'warning', string()} | {'error', string()}.
validate_request(_SeqNum, Count, _MaxSeqNum, MaxCount) when Count>MaxCount ->
    Fmt = "requested count ~p is bigger than configured maximum request count ~p",
    {warning, io_lib:format(Fmt, [Count, MaxCount])};
validate_request(SeqNum, _Count, _MaxSeqNum, _MaxCount) when SeqNum =< 0 ->
    Fmt = "requested sequence number ~p is smaller or equal to zero",
    {error, io_lib:format(Fmt, [SeqNum])};
validate_request(SeqNum, _Count, MaxSeqNum, _MaxCount) when SeqNum>MaxSeqNum ->
    Fmt = "requested sequence number ~p is bigger than any MoldUDP64 messages sent out (~p)",
    {error, io_lib:format(Fmt, [SeqNum, MaxSeqNum])};
validate_request(_SeqNum, _Count, _MaxSeqNum, _MaxCount) ->
    ok.

%% -------------------------------------------------------------
%% Given an io_device(), a position in bytes and a message block
%% count, returns the msg blocks in a list of binaries. Stop
%% adding msgs if their size would exceed MoldUDP64 packet size
%% ------------------------------------------------------------
-spec recover_messages(file:io_device(), non_neg_integer(), non_neg_integer(),
                       pos_integer(), pos_integer(), non_neg_integer(), [binary()]) ->
    {'ok', non_neg_integer(), [binary()]} | {'error', term()}.
recover_messages(_File, _Pos, 0, _MaxSize, _Size, NumMsgs, MsgBlocks) ->
    {ok, NumMsgs, lists:reverse(MsgBlocks)};
recover_messages(File, Pos, Count, MaxSize, Size, NumMsgs, MsgBlocks) ->
    case read_one_block(File, Pos) of
        {ok, Len, Msg} ->
            case Size+2+Len > MaxSize of
                true ->
                    {ok, NumMsgs, lists:reverse(MsgBlocks)};
                false ->
                    MsgBlock = <<Len:16, Msg:Len/binary>>,
                    recover_messages(File, Pos+2+Len, Count-1, MaxSize, Size+2+Len, NumMsgs+1, [MsgBlock|MsgBlocks])
            end;
        eof ->
            % client asked for more msg blocks than exist,
            % return what is there
            {ok, NumMsgs, lists:reverse(MsgBlocks)};
        {error, Reason} ->
            {error, Reason}
    end.

%% -------------------------------------------------------------
%% Reads a message at position Pos in the file, returns the length
%% of the message and the message itself
%% ------------------------------------------------------------
-spec read_one_block(file:io_device(), non_neg_integer()) ->
    {'ok', non_neg_integer(), binary()} | {'error', term()} | 'eof'.
read_one_block(File, Pos) ->
    case file:pread(File, Pos, 2) of
        {ok, <<Len:16/big-integer>>} ->
            case file:pread(File, Pos+2, Len) of
                {ok, <<Msg/binary>>} ->
                    {ok, Len, Msg};
                eof ->
                    {error, ill_ended_msg_store};
                {error, Reason} ->
                    {error, Reason}
            end;
        eof ->
            eof;
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

-endif.

