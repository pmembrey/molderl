
-module(molderl_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../src/molderl.hrl").

-define(MCAST_GROUP_IP, {239,192,42,69}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% FIXTURES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start() ->
    io:format(user,"Initiating tests...~n",[]),
    application:start(molderl),
    [].

stop(_) ->
    io:format(user,"Cleaning up...~n",[]),
    application:stop(molderl).

instantiator(_) ->
    FooPort = 7777,
    BarPort = 8888,
    BazPort = 9999,
    FooRecPort = 7778,
    BarRecPort = 8889,
    BazRecPort = 10000,

    {ok, [{LocalHostIP,_,_}|_]} = inet:getif(),

    % set up UDP multicast listen sockets
    {ok, FooSocket} = gen_udp:open(FooPort, [binary, {reuseaddr, true}]),
    inet:setopts(FooSocket, [{add_membership, {?MCAST_GROUP_IP, {127,0,0,1}}}]),
    {ok, BarSocket} = gen_udp:open(BarPort, [binary, {reuseaddr, true}]),
    inet:setopts(BarSocket, [{add_membership, {?MCAST_GROUP_IP, {127,0,0,1}}}]),
    {ok, BazSocket} = gen_udp:open(BazPort, [binary, {reuseaddr, true}]),
    inet:setopts(BazSocket, [{add_membership, {?MCAST_GROUP_IP, {127,0,0,1}}}]),

    {ok, FooPid} = molderl:create_stream(foo,?MCAST_GROUP_IP,FooPort,FooRecPort,LocalHostIP,100),
    {ok, BarPid} = molderl:create_stream(bar,?MCAST_GROUP_IP,BarPort,BarRecPort,LocalHostIP,100),
    {ok, BazPid} = molderl:create_stream(baz,?MCAST_GROUP_IP,BazPort,BazRecPort,LocalHostIP,200),
    ConflictAddr = molderl:create_stream(qux,?MCAST_GROUP_IP,BarPort,8890,LocalHostIP,100),
    ConflictPort = molderl:create_stream(bar,?MCAST_GROUP_IP,4321,BarRecPort,LocalHostIP,100),
    molderl:send_message(FooPid, <<"HelloWorld">>),
    [{Seq1, Msg1}] = receive_messages("foo", FooSocket),
    molderl:send_message(FooPid, <<"HelloWorld">>),
    [{Seq2, Msg2}] = receive_messages("foo", FooSocket),
    molderl:send_message(FooPid, <<"foo">>),
    molderl:send_message(FooPid, <<"bar">>),
    molderl:send_message(FooPid, <<"baz">>),
    Msgs = receive_messages("foo", FooSocket),
    molderl:send_message(FooPid, <<"foo">>),
    molderl:send_message(BarPid, <<"bar">>),
    molderl:send_message(BazPid, <<"baz">>),
    molderl:send_message(FooPid, <<"foo">>),
    molderl:send_message(BarPid, <<"bar">>),
    molderl:send_message(BazPid, <<"baz">>),
    BazMsgs = receive_messages("baz", BazSocket),
    FooMsgs = receive_messages("foo", FooSocket),
    BarMsgs = receive_messages("bar", BarSocket),
    BigMsg = list_to_binary([random:uniform(100) || _ <- lists:seq(1, ?PACKET_SIZE-200)]),
    molderl:send_message(BazPid, BigMsg),
    [{_,ExpectedBigMsg}] = receive_messages("baz", BazSocket),

    % Recovery tests

    % using same port for streaming and recovery
    SessionName = molderl_utils:gen_streamname("foo"),
    Request1 = <<SessionName/binary, Seq1:64, 1:16>>,
    gen_udp:send(FooSocket, LocalHostIP, FooRecPort, Request1),
    [RecoveredMsg1] = receive_messages("foo", FooSocket),
    Request2 = <<SessionName/binary, Seq2:64, 1:16>>,
    gen_udp:send(FooSocket, LocalHostIP, FooRecPort, Request2),
    [RecoveredMsg2] = receive_messages("foo", FooSocket),

    % using different ports for streaming and recovery
    QuxPort = 45678,
    {ok, QuxSocket} = gen_udp:open(QuxPort, [binary, {reuseaddr, true}]),
    gen_udp:send(QuxSocket, LocalHostIP, FooRecPort, Request1),
    [RecoveredMsg3] = receive_messages("foo", QuxSocket),
    gen_udp:send(QuxSocket, LocalHostIP, FooRecPort, Request2),
    [RecoveredMsg4] = receive_messages("foo", QuxSocket),

    [
        ?_assertEqual({error, destination_address_already_in_use}, ConflictAddr),
        ?_assertEqual({error, recovery_port_already_in_use}, ConflictPort),
        ?_assertEqual(<<"HelloWorld">>, Msg1),
        ?_assertEqual(<<"HelloWorld">>, Msg2),
        ?_assertMatch([{_,<<"foo">>},{_,<<"bar">>},{_,<<"baz">>}], Msgs),
        ?_assertMatch([{_,<<"bar">>},{_,<<"bar">>}], BarMsgs),
        ?_assertMatch([{_,<<"baz">>},{_,<<"baz">>}], BazMsgs),
        ?_assertMatch([{_,<<"foo">>},{_,<<"foo">>}], FooMsgs),
        ?_assertEqual(BigMsg, ExpectedBigMsg),
        ?_assertEqual({Seq1, Msg1}, RecoveredMsg1),
        ?_assertEqual({Seq2, Msg2}, RecoveredMsg2),
        ?_assertEqual({Seq1, Msg1}, RecoveredMsg3),
        ?_assertEqual({Seq2, Msg2}, RecoveredMsg4)
     ].

molderl_test_() ->
    {setup,
     fun start/0,
     fun stop/1,
     fun instantiator/1}.

receive_messages(StreamName, Socket) ->
    ModName = molderl_utils:gen_streamname(StreamName),
    ModNameSize = byte_size(ModName),
    receive
        {udp, Socket, _, _, <<ModName:ModNameSize/binary, _:80/integer>>} ->
            receive_messages(StreamName, Socket); % ignore heartbeats
        {udp, Socket, _, _, <<ModName:ModNameSize/binary, Tail/binary>>} ->
            <<NextSeq:64/big-integer, Count:16/big-integer, RawMsgs/binary>> = Tail,
            Msgs = [Msg || <<Size:16/big-integer, Msg:Size/binary>> <= RawMsgs],
            lists:zip(lists:seq(NextSeq, NextSeq+Count-1), Msgs)
    after
        1000 ->
            {error, timeout}
    end.

