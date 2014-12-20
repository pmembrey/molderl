
-module(molderl_integration_tests).

-export([launch/0]).

-include_lib("eunit/include/eunit.hrl").

-include("molderl_tests.hrl").

-compile([{parse_transform, lager_transform}]).

-record(stream, {pid :: pid(),
                 name :: string(),
                 socket :: inet:socket()}).

-record(state, {stream :: #stream{},
                inflight=sets:new() :: sets:set({pos_integer(), binary()}),
                seq_num=1 :: pos_integer()}).

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

    Stream = #stream{pid=Pid, socket=Socket, name="foo"},
    loop(#state{stream=Stream}, 2000).

loop(_State, 0) ->
    lager:info("[SUCCESS] Passed all tests!"),
    clean_up();
loop(State, NumTests) ->
    Fmt = "Number of tests left: ~p, Number of message in flight: ~p, number of messages received: ~p",
    lager:info(Fmt, [NumTests, sets:size(State#state.inflight), State#state.seq_num-1]),
    Draw = random:uniform(),
    if
        Draw < 0.8 ->
            TestResult = send(State);
        true ->
            TestResult = rcv(State)
    end,
    case TestResult of
        {passed, Outcome, NewState} ->
            lager:info(Outcome),
            loop(NewState, NumTests-1);
        {failed, Reason} ->
            lager:error(Reason),
            clean_up()
    end.

send(State) ->
    Msg = crypto:strong_rand_bytes(10), % generate random payload
    case molderl:send_message(State#state.stream#stream.pid, Msg) of
        ok ->
            Outcome = io_lib:format("[SUCCESS] Sent packet seq num: ~p, msg: ~p", [State#state.seq_num, Msg]),
            Inflight = sets:add_element({State#state.seq_num, Msg}, State#state.inflight),
            {passed, Outcome, State#state{inflight=Inflight, seq_num=State#state.seq_num+1}};
        _ ->
            Fmt = "[FAILURE] Couldn't send packet seq num: ~p, msg: ~p",
            Reason = io_lib:format(Fmt, [State#state.seq_num, Msg]),
            {failed, Reason}
    end.

rcv(State) ->
    Stream = State#state.stream,
    rcv(State, receive_messages(Stream#stream.name, Stream#stream.socket, 200)).

rcv(State, []) ->
    case sets:size(State#state.inflight) of
        0 ->
            Outcome = "[SUCCESS] Received all packets that were in flight",
            {passed, Outcome, State};
        NumInflights ->
            Fmt = "[FAILURE] Received ~p less packets that were in flight",
            {failed, io_lib:format(Fmt, [NumInflights])}
    end;
rcv(State, [Observed|Packets]) ->
    case sets:is_element(Observed, State#state.inflight) of
        true ->
            rcv(State#state{inflight=sets:del_element(Observed, State#state.inflight)}, Packets);
        false ->
            Fmt = "[FAILURE] Received packet not in flight: ~p",
            {failed, io_lib:format(Fmt, [Observed])}
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

