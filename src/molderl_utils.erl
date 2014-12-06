
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_utils).
-export([gen_heartbeat/2,
         gen_endofsession/2,
         gen_streamname/1,
         encode_messages/1,
         gen_messagepacket/4]).

-export([message_length/2,
         get_max_message_size/0]).

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
-spec encode_messages([binary()]) ->
    {[binary()], [non_neg_integer()], non_neg_integer(), non_neg_integer()}.
encode_messages(Msgs) ->
    encode_messages(Msgs, [], [], 0, 0).

-spec encode_messages([binary()], [binary()], [non_neg_integer()], non_neg_integer(), non_neg_integer()) ->
    {[binary()], [non_neg_integer()], non_neg_integer(), non_neg_integer()}.
encode_messages([], EncodedMsgs, EncodedMsgsSize, NumMsgs, NumBytes) ->
    {EncodedMsgs, EncodedMsgsSize, NumMsgs, NumBytes};
encode_messages([Msg|Msgs], EncodedMsgs, EncodedMsgsSize, NumMsgs, NumBytes) ->
    Length = byte_size(Msg),
    EncodedMsg = <<Length:16/big-integer, Msg/binary>>,
    encode_messages(Msgs,[EncodedMsg|EncodedMsgs],[Length|EncodedMsgsSize],NumMsgs+1,NumBytes+Length).

%% ------------------------------------------------
%% Given a parent message's length and a child
%% message (as a bitstring), returns what would
%% be the resulting message length if the child
%% message was to be appended to the parent message
%% (according to Mold 64 protocol)
%% ------------------------------------------------
-spec message_length(non_neg_integer(), binary()) -> pos_integer().
message_length(0,Message) ->
    % Header is 20 bytes
    % 2 bytes for length of message
    % X bytes for message
    22 + byte_size(Message);
message_length(Size,Message) ->
    % Need to add 2 bytes for the length
    % and X bytes for the message itself
    Size + 2 + byte_size(Message).

%% ---------------------------------------------------
%% Return the maximum payload size. Currently this
%% is hard coded in a header, but ultimately this will
%% be configurable in order to support jumbo frames or
%% to allow packets that can fragment.
%%
%% As molderl will crash if an app tries to send a
%% message that's larger than the maximum size, there
%% needs to be a way to query for that size.
%% ----------------------------------------------------
-spec get_max_message_size() -> pos_integer().
get_max_message_size() ->
    ?PACKET_SIZE.

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
%% Tests for message length computing
%% -----------------------------------
message_length_test() ->
    ?assertEqual(22, message_length(0,<<>>)),
    ?assertEqual(25, message_length(0,<<"f","o","o">>)),
    ?assertEqual(3, message_length(1,<<>>)),
    ?assertEqual(6, message_length(1,<<"f","o","o">>)).

%% -----------------------------------
%% Tests for encoding messages
%% -----------------------------------
encode_messages_test() ->
    ?assertEqual(encode_messages([<<"foo">>,<<"bar">>,<<"quux">>]),
                 {[<<4:16/big-integer, <<"quux">>/binary>>,
                   <<3:16/big-integer, <<"bar">>/binary>>,
                   <<3:16/big-integer, <<"foo">>/binary>>], [4,3,3], 3, 10}).

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

-endif.

