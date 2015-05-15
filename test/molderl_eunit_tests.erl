
-module(molderl_eunit_tests).

-include_lib("eunit/include/eunit.hrl").

-include("molderl.hrl").
-include("molderl_tests.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% FIXTURES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start() ->
    io:format(user,"Initiating tests...~n",[]),
    file:delete("/tmp/foo"),
    file:delete("/tmp/bar"),
    file:delete("/tmp/baz"),
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

    {ok, FooPid} = molderl:create_stream(foo,?MCAST_GROUP_IP,FooPort,FooRecPort,[{filename,"/tmp/foo"}]),
    {ok, BarPid} = molderl:create_stream(bar,?MCAST_GROUP_IP,BarPort,BarRecPort,[{ipaddresstosendfrom,LocalHostIP},{filename,"/tmp/bar"}]),
    {ok, BazPid} = molderl:create_stream(baz,?MCAST_GROUP_IP,BazPort,BazRecPort,[{filename,"/tmp/baz"},{timer,200}]),
    ConflictAddr = molderl:create_stream(qux,?MCAST_GROUP_IP,BarPort,8890,[{ipaddresstosendfrom,LocalHostIP},{timer,100}]),
    ConflictPort = molderl:create_stream(bar,?MCAST_GROUP_IP,4321,BarRecPort,[{ipaddresstosendfrom,LocalHostIP}]),

    molderl:send_message(FooPid, <<"HelloWorld">>),
    {ok, [{Seq1, Msg1}]} = receive_messages("foo", FooSocket, 500),
    molderl:send_message(FooPid, <<"HelloWorld">>),
    {ok, [{Seq2, Msg2}]} = receive_messages("foo", FooSocket, 500),
    molderl:send_message(FooPid, <<"foo">>),
    molderl:send_message(FooPid, <<"bar">>),
    molderl:send_message(FooPid, <<"baz">>),
    {ok, Msgs} = receive_messages("foo", FooSocket, 500),
    molderl:send_message(FooPid, <<"foo">>),
    molderl:send_message(BarPid, <<"bar">>),
    molderl:send_message(BazPid, <<"baz">>),
    molderl:send_message(FooPid, <<"foo">>),
    molderl:send_message(BarPid, <<"bar">>),
    molderl:send_message(BazPid, <<"baz">>),
    {ok, BazMsgs} = receive_messages("baz", BazSocket, 500),
    {ok, FooMsgs} = receive_messages("foo", FooSocket, 500),
    {ok, BarMsgs} = receive_messages("bar", BarSocket, 500),
    BigMsg = list_to_binary([random:uniform(100) || _ <- lists:seq(1, ?PACKET_SIZE-200)]),
    molderl:send_message(BazPid, BigMsg),
    {ok, [{_,ExpectedBigMsg}]} = receive_messages("baz", BazSocket, 500),

    % by now, Foo stream is like this:
    % [<<"HelloWorld">>, <<"HelloWorld">>, <<"foo">>, <<"bar">>, <<"baz">>, <<"foo">>, <<"foo">>]

    % Recovery tests

    % first send a broken recovery request, see if it breaks
    SessionName = molderl_utils:gen_streamname("foo"),
    BrokenRequest = <<SessionName/binary>>,
    gen_udp:send(FooSocket, LocalHostIP, FooRecPort, BrokenRequest),

    % using same port for streaming and recovery
    Request1 = <<SessionName/binary, Seq1:64, 1:16>>,
    gen_udp:send(FooSocket, LocalHostIP, FooRecPort, Request1),
    {ok, [RecoveredMsg1]} = receive_messages("foo", FooSocket, 500),
    Request2 = <<SessionName/binary, Seq2:64, 1:16>>,
    gen_udp:send(FooSocket, LocalHostIP, FooRecPort, Request2),
    {ok, [RecoveredMsg2]} = receive_messages("foo", FooSocket, 500),

    % test recovery multiple msgs
    Seq3 = Seq2+1,
    Request3 = <<SessionName/binary, Seq3:64, 3:16>>,
    gen_udp:send(FooSocket, LocalHostIP, FooRecPort, Request3),
    {ok, RecoveredMsgs3} = receive_messages("foo", FooSocket, 500),
    
    % using different ports for streaming and recovery
    QuxPort = 45678,
    {ok, QuxSocket} = gen_udp:open(QuxPort, [binary, {reuseaddr, true}]),
    gen_udp:send(QuxSocket, LocalHostIP, FooRecPort, Request1),
    {ok, [RecoveredMsg4]} = receive_messages("foo", QuxSocket, 500),
    gen_udp:send(QuxSocket, LocalHostIP, FooRecPort, Request2),
    {ok, [RecoveredMsg5]} = receive_messages("foo", QuxSocket, 500),

    % test when requested sequence number > total number of msgs sent
    Request6a = <<SessionName/binary, 100:64, 1:16>>,
    gen_udp:send(FooSocket, LocalHostIP, FooRecPort, Request6a),
    Result6a = receive_messages("foo", FooSocket, 500),

    % test when requested sequence number <= 0
    Request6b = <<SessionName/binary, 0:64, 1:16>>,
    gen_udp:send(FooSocket, LocalHostIP, FooRecPort, Request6b),
    Result6b = receive_messages("foo", FooSocket, 500),

    % test when requested count > max recovery count
    Request6c = <<SessionName/binary, 1:64, 5000:16>>,
    gen_udp:send(FooSocket, LocalHostIP, FooRecPort, Request6c),
    Result6c = receive_messages("foo", FooSocket, 500),

    % test when requested sequence number + requested count > total number of msgs sent
    Request7 = <<SessionName/binary, 6:64, 100:16>>,
    gen_udp:send(FooSocket, LocalHostIP, FooRecPort, Request7),
    {ok, RecoveredMsgs7} = receive_messages("foo", FooSocket, 500),

    % test when requested count is zero
    Request8 = <<SessionName/binary, 2:64, 0:16>>,
    gen_udp:send(FooSocket, LocalHostIP, FooRecPort, Request8),
    Result8 = receive_messages("foo", FooSocket, 500),

    % test when requested sequence number starts at very last message
    Request9 = <<SessionName/binary, 7:64, 8:16>>,
    gen_udp:send(FooSocket, LocalHostIP, FooRecPort, Request9),
    {ok, RecoveredMsgs9} = receive_messages("foo", FooSocket, 500),

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
        ?_assertEqual([{Seq3, <<"foo">>}, {Seq3+1, <<"bar">>}, {Seq3+2, <<"baz">>}], RecoveredMsgs3),
        ?_assertEqual({Seq1, Msg1}, RecoveredMsg4),
        ?_assertEqual({Seq2, Msg2}, RecoveredMsg5),
        ?_assertEqual({error, timeout}, Result6a),
        ?_assertEqual({error, timeout}, Result6b),
        ?_assertEqual({error, timeout}, Result6c),
        ?_assertEqual([{6, <<"foo">>}, {7, <<"foo">>}], RecoveredMsgs7),
        ?_assertEqual({error, timeout}, Result8),
        ?_assertEqual([{7, <<"foo">>}], RecoveredMsgs9)
     ].

molderl_test_() ->
    {setup,
     fun start/0,
     fun stop/1,
     fun instantiator/1}.

