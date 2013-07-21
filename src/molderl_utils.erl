-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_utils).
-export([gen_heartbeat/2,gen_endofsession/2,gen_streamname/1,gen_messagepacket/3]).
-include("molderl.hrl").


%% ------------------------------
%% Generates a Heart Beat packet.
%% ------------------------------
gen_heartbeat(StreamName,NextSeq) ->
  <<StreamName/binary,NextSeq:64/big-integer,?HEARTBEAT:16/big-integer>>.

%% -----------------------------------
%% Generates an End Of Session packet.
%% -----------------------------------
gen_endofsession(StreamName,NextSeq) ->
  <<StreamName/binary,NextSeq:64/big-integer,?END_OF_SESSION:16/big-integer>>.


%% ---------------------------------------------
%% Takes a stream name as either a list, binary
%% or an integer. It then converts to a binary
%% that is right padded with spaces (ala NASDAQ)
%% ---------------------------------------------
gen_streamname(StreamName) when is_binary(StreamName) == true ->
  gen_streamname(binary_to_list(StreamName));
gen_streamname(StreamName) when is_integer(StreamName) == true ->
  gen_streamname(integer_to_list(StreamName));
gen_streamname(StreamName) when is_list(StreamName) == true ->
  %First take the first 10 characters
  case length(StreamName) > 10 of
    true ->  {FirstTen,_TheRest} = lists:split(10,StreamName);
    false -> FirstTen = StreamName
  end,
  % Convert to a binary
  binary_padder(list_to_binary(FirstTen)).

%% --------------------------------------------
%% Takes a binary and pads it out to ten bytes.
%% This is needed by the Stream Name.
%% Doesn't handle binaries larger than 10, but
%% should never get called for those ;-)
%% --------------------------------------------
binary_padder(BinaryToPad) ->
  case byte_size(BinaryToPad) < 10 of
    true -> binary_padder(<<BinaryToPad/binary,16#20:8/big-integer>>);
    false -> BinaryToPad
  end.

%% -------------------------------------------------
%% Generates a message packet. This takes the stream
%% name, the next sequence number and a message (or 
%% list of messages). It returns a completed MOLD64
%% packet as well as the next sequence number. This
%% is needed for generating the next message in the
%% stream.
%% -------------------------------------------------
gen_messagepacket(StreamName,NextSeq,Message) when is_list(Message) == false ->
    gen_messagepacket(StreamName,NextSeq,[Message]);
gen_messagepacket(StreamName,NextSeq,Messages) ->
  EncodedMessages = lists:map(fun encode_message/1,Messages),
  io:format("~p~n",[EncodedMessages]),
  % Next Serial number is...
  Count = length(EncodedMessages),
  NewNextSeq = NextSeq + Count,
  io:format("Count: ~p~n",[Count]),
  FlattenedMessages = list_to_binary(lists:flatten(EncodedMessages)),
  PacketPayload = <<StreamName/binary,NextSeq:64/big-integer,Count:16/big-integer,FlattenedMessages/binary>>,
  {NewNextSeq,PacketPayload}.

%% ------------------------------------------------
%% Takes a message as either a list of a binary and
%% then adds the length header needed by MOLD64. It
%% returns the binary encoded message.
%% ------------------------------------------------
encode_message(Message) when is_list(Message) == true ->
  encode_message(list_to_binary(Message));
encode_message(Message) when is_binary(Message) == true ->
  Length = byte_size(Message),
  <<Length:16/big-integer,Message/binary>>.

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
-endif.
