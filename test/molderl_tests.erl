
-module(molderl_tests).

-include_lib("eunit/include/eunit.hrl").

-define(MCAST_GROUP_IP, {239,192,42,69}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% FIXTURES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start() ->
    io:format(user,"Initiating tests...~n",[]),
    application:start(molderl),
    [].
 
stop(_) ->
    io:format(user,"Cleaning up...~n",[]).
 
instantiator(_) ->
    FooPort = 7777,
    BarPort = 8888,
    BazPort = 9999,
    {ok, [{LocalHostIP,_,_}|_]} = inet:getif(),
    {ok, FooSocket} = gen_udp:open(FooPort, [binary]),
    {ok, BarSocket} = gen_udp:open(BarPort, [binary]),
    {ok, BazSocket} = gen_udp:open(BazPort, [binary]),
    CreateResult1 = molderl:create_stream(foo,"foo",?MCAST_GROUP_IP,FooPort,LocalHostIP,200),
    CreateResult2 = molderl:create_stream(bar,"bar",?MCAST_GROUP_IP,BarPort,LocalHostIP,200),
    CreateResult3 = molderl:create_stream(baz,"baz",?MCAST_GROUP_IP,BazPort,LocalHostIP,200),
    molderl:send_message(foo, <<"HelloWorld">>),
    [Msg1] = receive_message("foo", FooSocket),
    [
        ?_assertEqual(ok, CreateResult1),
        ?_assertEqual(ok, CreateResult2),
        ?_assertEqual(ok, CreateResult3),
        ?_assertEqual(<<"HelloWorld">>, Msg1)
    ].

molderl_test_() ->
    {setup,
     fun start/0,
     fun stop/1,
     fun instantiator/1}.

receive_message(StreamName, Socket) ->
    ModName = molderl_utils:gen_streamname(StreamName),
    receive
        {udp, Socket, _FromIp, _FromPort, Packet} ->
            ModNameSize = byte_size(ModName),
            <<ModName:ModNameSize/binary,
              _NextSeq:64/big-integer,
              _Count:16/big-integer,
              Msgs/binary>> = Packet,
              [Msg || <<Size:16/big-integer, Msg:Size/binary>> <= Msgs]
    after
        250 ->
            {error, timeout}
    end.

