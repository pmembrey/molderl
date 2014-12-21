
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_recovery).

-behaviour(gen_server).

-export([start_link/6, store/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-compile([{parse_transform, lager_transform}]).

-define(STATE,State#state).

-record(state, {
                socket :: port(),                 % Socket to send data on
                stream_name :: binary(),          % Stream name for encoding the response
                last_seq_num :: pos_integer(),    % sequence number of last msg block stored
                packet_size :: pos_integer(),     % maximum packet size of messages in bytes
                blocks_store :: file:io_device(), % file handle to MOLD message blocks store
                store_size :: non_neg_integer(),  % size on disk of msg blocks store
                index :: [integer()],             % indices to MOLD message blocks in above store
                % cache the StatsD keys to prevent repeated atom_to_list/1 calls and concatenation
                statsd_latency_key :: string(),
                statsd_count_key :: string()
               }).

start_link(StreamName, RecoveryPort, FileName, FileSize, Index, PacketSize) ->
    gen_server:start_link(?MODULE, [StreamName, RecoveryPort, FileName, FileSize, Index, PacketSize], []).

-spec store(pid(), [binary()], [non_neg_integer()], non_neg_integer()) -> ok.
store(Pid, Msgs, MsgsSize, NumMsgs) ->
    gen_server:cast(Pid, {store, Msgs, MsgsSize, NumMsgs}).

init([StreamName, RecoveryPort, FileName, FileSize, Index, PacketSize]) ->

    process_flag(trap_exit, true), % so that terminate/2 gets called when process exits

    {ok, Socket} = gen_udp:open(RecoveryPort, [binary, {active,once}]),

    case file:open(FileName, [read, append, raw, binary, read_ahead, sync]) of
        {ok, IoDevice} ->
            State = #state{socket             = Socket,
                           stream_name        = molderl_utils:gen_streamname(StreamName),
                           last_seq_num       = length(Index),
                           packet_size        = PacketSize,
                           blocks_store       = IoDevice,
                           store_size         = FileSize,
                           index              = Index,
                           statsd_latency_key = "molderl." ++ atom_to_list(StreamName) ++ ".recovery_request.latency",
                           statsd_count_key   = "molderl." ++ atom_to_list(StreamName) ++ ".recovery_request.received"},
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_cast({store, Msgs, MsgsSize, NumMsgs}, State) ->
    ok = file:write(?STATE.blocks_store, Msgs),
    {Positions, NewFileSize} = lists:mapfoldl(fun(M, S) -> {S, S+M} end, ?STATE.store_size, MsgsSize),
    {noreply, ?STATE{index=?STATE.index++Positions,
                   last_seq_num=?STATE.last_seq_num+NumMsgs,
                   store_size=NewFileSize}}.

handle_call(Msg, _From, State) ->
    lager:warning("[molderl] Unexpected message in module ~p: ~p",[?MODULE, Msg]),
    {noreply, State}.

handle_info({udp, _Client, IP, Port, <<SessionName:10/binary,SequenceNumber:64/big-integer,Count:16/big-integer>>}, State) ->
    TS = os:timestamp(),
    Fmt = "[molderl] Received recovery request from ~p: [session name] ~p [sequence number] ~p [count] ~p",
    lager:debug(Fmt, [IP,string:strip(binary_to_list(SessionName), right), SequenceNumber, Count]),

    % First check recovery request is valid
    case SequenceNumber < 1 orelse SequenceNumber > ?STATE.last_seq_num of
        true -> % can't request for sequence number bigger what's been stored...
            Fmt2 = "[molderl] invalid recovery request: requested sequence number: ~p, max sequence number: ~p",
            lager:warning(Fmt2, [SequenceNumber, ?STATE.last_seq_num]);
        false -> % recover msgs from store and send

            Position = lists:nth(SequenceNumber, ?STATE.index),
            {ok, Messages} = recover_messages(?STATE.blocks_store, Position, Count),

            % Remove messages if bigger than allowed packet size
            {NumMsgs, TruncatedMsgs} = truncate_messages(Messages, ?STATE.packet_size),
            Payload = molderl_utils:gen_messagepacket(?STATE.stream_name, SequenceNumber, NumMsgs, TruncatedMsgs),

            ok = gen_udp:send(?STATE.socket, IP, Port, Payload),
            lager:debug("[molderl] Replied recovery request from ~p - reply contains ~p messages", [IP, NumMsgs])
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

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, State) ->
    Fmt = "[molderl] recovery process for stream ~p is exiting because of reason ~p.",
    lager:info(Fmt, [string:strip(binary_to_list(?STATE.stream_name)), Reason]),
    file:sync(?STATE.blocks_store),
    file:close(?STATE.blocks_store),
    ok = gen_udp:close(?STATE.socket).

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
    Packet = truncate_messages(Messages, 33),
    Expected = {4, [<<>>, <<"x">>, <<"a","b","c","d","e">>, <<"1","2","3">>]},
    ?assertEqual(Packet, Expected).

truncate_messages_empty_test() ->
    Packet = truncate_messages([], 40),
    ?assertEqual(Packet, {0, []}).

-endif.

