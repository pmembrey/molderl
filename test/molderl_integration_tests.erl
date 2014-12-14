
-module(molderl_integration_tests).

-export([launch/0]).

-include_lib("eunit/include/eunit.hrl").

-include("molderl_tests.hrl").

-compile([{parse_transform, lager_transform}]).

-record(stream, {pid :: pid(),
                 socket :: inet:socket(),
                 seq_num=1 :: pos_integer(),
                 name :: string()}).

launch() ->

    File = "/tmp/foo",
    Port = 6666,
    RecPort = 7777,

    {ok, [{LocalHostIP,_,_}|_]} = inet:getif(),
    file:delete(File),
    lager:start(),
    lager:set_loglevel(lager_console_backend, info),
    application:start(molderl),

    {ok, Socket} = gen_udp:open(Port, [binary, {reuseaddr, true}]),
    inet:setopts(Socket, [{add_membership, {?MCAST_GROUP_IP, {127,0,0,1}}}]),

    {ok, Pid} = molderl:create_stream(foo,?MCAST_GROUP_IP,Port,RecPort,LocalHostIP,File,100),

    loop(#stream{pid=Pid, socket=Socket, name="foo"}, 50).

loop(_Stream, 0) ->
    clean_up();
loop(Stream, NumTests) ->
    case test_send_and_receive(Stream) of
        {passed, NewStream} ->
            loop(NewStream, NumTests-1);
        {failed, _Reason} ->
            clean_up()
    end.

test_send_and_receive(Stream) ->
    Msg = <<"message01">>,
    molderl:send_message(Stream#stream.pid, Msg),
    Expected = [{Stream#stream.seq_num, Msg}],
    case receive_messages(Stream#stream.name, Stream#stream.socket, 500) of
        Expected ->
            lager:info("[SUCCESS] Passed a send and receive test"),
            {passed, Stream#stream{seq_num=Stream#stream.seq_num+1}};
        Observed ->
            Fmt = "[FAILURE] Test send and receive failed - expected: ~p, observed: ~p",
            lager:error(Fmt, [Expected, Observed]),
            {failed, "failed send and receive test"}
    end.

clean_up() ->
    application:stop(molderl).

%    molderl:send_message(Pid, <<"message01">>),
%    molderl:send_message(Pid, <<"message02">>),
%    molderl:send_message(Pid, <<"message03">>),
%    molderl:send_message(Pid, <<"message04">>),
%    molderl:send_message(Pid, <<"message05">>),
%    molderl:send_message(Pid, <<"message06">>),
%    molderl:send_message(Pid, <<"message07">>),
%    molderl:send_message(Pid, <<"message08">>),
%    molderl:send_message(Pid, <<"message09">>),
%    molderl:send_message(Pid, <<"message10">>),
%    molderl:send_message(Pid, <<"message11">>),
%    molderl:send_message(Pid, <<"message12">>),
%
%    Expected1 = [{1,<<"message01">>},{2,<<"message02">>},{3,<<"message03">>},{4,<<"message04">>},
%                 {5,<<"message05">>},{6,<<"message06">>},{7,<<"message07">>},{8,<<"message08">>},
%                 {9,<<"message09">>},{10,<<"message10">>},{11,<<"message11">>},{12,<<"message12">>}],
%    Observed1 = receive_messages("foo", Socket, 500),
%    ?_assertEqual(Observed1, Expected1),
%
%    StreamName = molderl_utils:gen_streamname("foo"),
%
%    Request1 = <<StreamName/binary, 1:64, 1:16>>,
%    gen_udp:send(Socket, LocalHostIP, RecPort, Request1),
%    [RecoveredMsg1] = receive_messages("foo", Socket, 500),
%    ?_assertEqual(RecoveredMsg1, {1, <<"message01">>}),
%
%    Request2 = <<StreamName/binary, 2:64, 1:16>>,
%    gen_udp:send(Socket, LocalHostIP, RecPort, Request2),
%    [RecoveredMsg2] = receive_messages("foo", Socket, 500),
%    ?_assertEqual(RecoveredMsg2, {2, <<"message02">>}),
%
%    Request3 = <<StreamName/binary, 3:64, 2:16>>,
%    gen_udp:send(Socket, LocalHostIP, RecPort, Request3),
%    [RecoveredMsg3, RecoveredMsg4] = receive_messages("foo", Socket, 500),
%    ?_assertEqual(RecoveredMsg3, {3, <<"message03">>}),
%    ?_assertEqual(RecoveredMsg4, {4, <<"message04">>}).

