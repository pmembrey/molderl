
-module(molderl_integration_tests).

-export([test/0]).

-include_lib("eunit/include/eunit.hrl").

-include("molderl_tests.hrl").

test() ->

    application:start(molderl),
    lager:start(),

    File = "/tmp/foo",
    Port = 6666,

    {ok, [{LocalHostIP,_,_}|_]} = inet:getif(),
    file:delete(File),

    {ok, Socket} = gen_udp:open(Port, [binary, {reuseaddr, true}]),
    inet:setopts(Socket, [{add_membership, {?MCAST_GROUP_IP, {127,0,0,1}}}]),

    {ok, Pid} = molderl:create_stream(foo,?MCAST_GROUP_IP,Port,7777,LocalHostIP,File,100),

    molderl:send_message(Pid, <<"message01">>),
    molderl:send_message(Pid, <<"message02">>),
    molderl:send_message(Pid, <<"message03">>),
    molderl:send_message(Pid, <<"message04">>),
    molderl:send_message(Pid, <<"message05">>),
    molderl:send_message(Pid, <<"message06">>),
    molderl:send_message(Pid, <<"message07">>),
    molderl:send_message(Pid, <<"message08">>),
    molderl:send_message(Pid, <<"message09">>),
    molderl:send_message(Pid, <<"message10">>),
    molderl:send_message(Pid, <<"message11">>),
    molderl:send_message(Pid, <<"message12">>),

    Expected = [{1,<<"message01">>},{2,<<"message02">>},{3,<<"message03">>},{4,<<"message04">>},
                {5,<<"message05">>},{6,<<"message06">>},{7,<<"message07">>},{8,<<"message08">>},
                {9,<<"message09">>},{10,<<"message10">>},{11,<<"message11">>},{12,<<"message12">>}],

    ?_assertEqual(receive_messages("foo", Socket, 500), Expected).

