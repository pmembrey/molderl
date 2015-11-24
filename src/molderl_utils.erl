
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_utils).
-export([gen_heartbeat/2,
         gen_endofsession/2,
         gen_streamname/1,
         encode_messages/1,
         gen_messagepacket/4,
         gen_streamprocessname/1,
         gen_recoveryprocessname/1]).

-include("molderl.hrl").

%% ------------------------------
%% Generates a Heart Beat packet.
%% ------------------------------
-spec gen_heartbeat(binary(), pos_integer()) -> binary().
gen_heartbeat(StreamName,NextSeq) ->
    <<StreamName/binary,NextSeq:64/big-integer,?HEARTBEAT:16/big-integer>>.

%% -----------------------------------
%% Generates an End Of Session packet.
%% -----------------------------------
-spec gen_endofsession(binary(), pos_integer()) -> binary().
gen_endofsession(StreamName,NextSeq) ->
    <<StreamName/binary,NextSeq:64/big-integer,?END_OF_SESSION:16/big-integer>>.

%% ---------------------------------------------
%% Takes a stream name as either a list, binary
%% or an integer. It then converts to a binary
%% that is right padded with spaces (ala NASDAQ)
%% ---------------------------------------------
-spec gen_streamname(atom() | binary() | string() | integer()) -> binary().
gen_streamname(StreamName) when is_atom(StreamName) ->
    gen_streamname(atom_to_list(StreamName));
gen_streamname(StreamName) when is_binary(StreamName) ->
    gen_streamname(binary_to_list(StreamName));
gen_streamname(StreamName) when is_integer(StreamName) ->
    gen_streamname(integer_to_list(StreamName));
gen_streamname(StreamName) when is_list(StreamName), length(StreamName) > 10 ->
    %First take the first 10 characters
    {FirstTen,_TheRest} = lists:split(10,StreamName),
    binary_padder(list_to_binary(FirstTen));
gen_streamname(StreamName) when is_list(StreamName) ->
    binary_padder(list_to_binary(StreamName)).

-spec gen_streamprocessname(atom()) -> atom().
gen_streamprocessname(StreamName) ->
    list_to_atom(atom_to_list(mold_stream_) ++ atom_to_list(StreamName)).

-spec gen_recoveryprocessname(atom()) -> atom().
gen_recoveryprocessname(StreamName) ->
    list_to_atom(atom_to_list(mold_recovery_) ++ atom_to_list(StreamName)).
%% --------------------------------------------
%% Takes a binary and pads it out to ten bytes.
%% This is needed by the Stream Name.
%% Doesn't handle binaries larger than 10, but
%% should never get called for those ;-)
%% --------------------------------------------
-spec binary_padder(binary()) -> binary().
binary_padder(BinaryToPad) when byte_size(BinaryToPad) < 10 ->
    binary_padder(<<BinaryToPad/binary,16#20:8/big-integer>>);
binary_padder(BinaryToPad) ->
    BinaryToPad.

-spec gen_messagepacket(binary(), pos_integer(), pos_integer(), [binary()]) -> binary().
gen_messagepacket(StreamName, NextSeq, NumMsgs, EncodedMsgs) ->
    EncodedMsgsBinary = list_to_binary(EncodedMsgs),
    <<StreamName/binary, NextSeq:64, NumMsgs:16, EncodedMsgsBinary/binary>>.

%% ------------------------------------------------
%% Takes a list of binary messages, prepend the
%% length header needed by MOLD64 to each of them
%% and returns the resulting list of encoded msgs,
%% plus list of encoded msgs sizes, total number
%% of msgs and total byte size.
%% ------------------------------------------------
-spec encode_messages([binary()]) -> {[binary()], [non_neg_integer()], non_neg_integer()}.
encode_messages(Msgs) ->
    encode_messages(Msgs, [], [], 0).

-spec encode_messages([binary()], [binary()], [non_neg_integer()], non_neg_integer()) ->
    {[binary()], [non_neg_integer()], non_neg_integer()}.
encode_messages([], EncodedMsgs, EncodedMsgsSize, NumMsgs) ->
    {EncodedMsgs, EncodedMsgsSize, NumMsgs};
encode_messages([Msg|Msgs], EncodedMsgs, EncodedMsgsSize, NumMsgs) ->
    Length = byte_size(Msg),
    EncodedMsg = <<Length:16/big-integer, Msg/binary>>,
    encode_messages(Msgs, [EncodedMsg|EncodedMsgs], [Length+2|EncodedMsgsSize], NumMsgs+1).

-ifdef(TEST).

%% -----------------------
%% Tests for binary_padder
%% -----------------------
binary_padder_empty_test() ->
  ?assert(binary_padder(<<>>) == <<"          ">>).
binary_padder_short_test() ->
  ?assert(binary_padder(<<"hello">>) == <<"hello     ">>).
binary_padder_ten_test() ->
  ?assert(binary_padder(<<"helloworld">>) == <<"helloworld">>).

%% ------------------------
%% Tests for gen_streamname
%% ------------------------
gen_streamname_integer_test() ->
  ?assert(gen_streamname(1234) == <<"1234      ">>).
gen_streamname_binary_test() ->
  ?assert(gen_streamname(<<"1234">>) == <<"1234      ">>).
gen_streamname_short_list_test() ->
  ?assert(gen_streamname("hello") == <<"hello     ">>).
gen_streamname_long_list_test() ->
  ?assert(gen_streamname("helloworld123") == <<"helloworld">>).
gen_streamname_ten_list_test() ->
  ?assert(gen_streamname("helloworld") == <<"helloworld">>).

%% -------------------------------
%% Tests for Heart Beat generation
%% -------------------------------
gen_heartbeat_test() ->
  StreamName = <<"helloworld">>,
  ?assert(gen_heartbeat(StreamName,10) == <<StreamName/binary,10:64/big-integer,?HEARTBEAT:16/big-integer>>).

%% -----------------------------------
%% Tests for End of Session generation
%% -----------------------------------
gen_endofsession_test() ->
  StreamName = <<"helloworld">>,
  ?assert(gen_endofsession(StreamName,20) == <<StreamName/binary,20:64/big-integer,?END_OF_SESSION:16/big-integer>>).

%% -----------------------------------
%% Tests for encoding messages
%% -----------------------------------
encode_messages_test() ->
    ?assertEqual(encode_messages([<<"foo">>,<<"bar">>,<<"quux">>]),
                 {[<<4:16/big-integer, <<"quux">>/binary>>,
                   <<3:16/big-integer, <<"bar">>/binary>>,
                   <<3:16/big-integer, <<"foo">>/binary>>], [6,5,5], 3}).

%%% -----------------------------------
%%% Tests for generating message packet
%%% -----------------------------------

gen_messagepacket_test() ->
    Msgs = [<<"foo">>, <<"bar">>, <<"quux">>],
    Observed = gen_messagepacket(<<"foo">>, 23, length(Msgs), Msgs),
    Expected = <<<<"foo">>/binary, 23:64, 3:16, <<"foobarquux">>/binary>>,
    ?assertEqual(Expected, Observed).

gen_messagepacket_empty_test() ->
    Msgs = [],
    Observed = gen_messagepacket(<<"foo">>, 23, length(Msgs), Msgs),
    Expected = <<<<"foo">>/binary, 23:64, 0:16>>,
    ?assertEqual(Expected, Observed).

%% -----------------------
%% Tests for gen_streamprocessname
%% -----------------------
gen_streamprocessname_test() ->
  ?assert(gen_streamprocessname(abcd) == mold_stream_abcd).

%% -----------------------
%% Tests for gen_recoveryprocessname
%% -----------------------
gen_recoveryprocessname_test() ->
  ?assert(gen_recoveryprocessname(abcd) == mold_recovery_abcd).
-endif.

