-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl_utils).
-export([gen_heartbeat/2,gen_endofsession/2]).
-include("molderl.hrl").

gen_heartbeat(StreamName,NextSeq) ->
  <<StreamName/binary,NextSeq:64/big-integer,?HEARTBEAT:16/big-integer>>.

gen_endofsession(StreamName,NextSeq) ->
  <<StreamName/binary,NextSeq:64/big-integer,?END_OF_SESSION:16/big-integer>>.
