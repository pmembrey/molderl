
-define(MCAST_GROUP_IP, {239,192,42,69}).

receive_messages(_StreamName, _Socket, Timeout) when Timeout =< 0 ->
    {error, timeout};
receive_messages(StreamName, Socket, Timeout) ->
    Now = os:timestamp(),
    ModName = molderl_utils:gen_streamname(StreamName),
    ModNameSize = byte_size(ModName),
    receive
        {udp, Socket, _, _, <<ModName:ModNameSize/binary, _:80/integer>>} -> % ignore heartbeats
            receive_messages(StreamName, Socket, Timeout-timer:now_diff(os:timestamp(), Now));
        {udp, Socket, _, _, <<ModName:ModNameSize/binary, Tail/binary>>} ->
            <<NextSeq:64/big-integer, Count:16/big-integer, RawMsgs/binary>> = Tail,
            Msgs = [Msg || <<Size:16/big-integer, Msg:Size/binary>> <= RawMsgs],
            {ok, lists:zip(lists:seq(NextSeq, NextSeq+Count-1), Msgs)}
    after
        Timeout ->
            {error, timeout}
    end.

