
-module(molderl_integration_tests).

-export([launch/0]).

-include_lib("eunit/include/eunit.hrl").

-include("molderl_tests.hrl").

-compile([{parse_transform, lager_transform}]).

-record(stream, {pid :: pid(),
                 name :: string(),
                 socket :: inet:socket(),
                 ip :: inet:ip4_address(),
                 recovery_port :: inet:port_number()}).

-record(state, {stream :: #stream{},
                sent=[] :: [{pos_integer(), binary()}],
                inflight=[] :: [{pos_integer(), binary()}],
                seq_num=1 :: pos_integer(),
                num_msgs_rcvd=0 :: non_neg_integer()}).

launch() ->

    File = "/tmp/foo",
    Port = 6666,
    RecPort = 7777,

    {ok, [{LocalHostIP,_,_}|_]} = inet:getif(),
    file:delete(File),
    lager:start(),
    lager:set_loglevel(lager_console_backend, debug),
    application:start(molderl),

    {ok, Socket} = gen_udp:open(Port, [binary, {reuseaddr, true}]),
    inet:setopts(Socket, [{add_membership, {?MCAST_GROUP_IP, {127,0,0,1}}}]),

    {ok, Pid} = molderl:create_stream(foo,?MCAST_GROUP_IP,Port,RecPort,LocalHostIP,File,50),

    Stream = #stream{pid=Pid, name="foo", socket=Socket, ip=LocalHostIP, recovery_port=RecPort},
    loop(#state{stream=Stream}, 10).

loop(#state{inflight=[]}, 0) ->
    lager:info("[SUCCESS] Passed all tests!"),
    clean_up();
loop(State, 0) ->
    Fmt = "No more tests left but still ~p messages in flight, making sure we receive them all",
    lager:info(Fmt, [length(State#state.inflight)]),
    case rcv(State) of
        {passed, Outcome, NewState} ->
            lager:info(Outcome),
            loop(NewState, 0);
        {failed, Reason} ->
            lager:error(Reason),
            clean_up()
    end;
loop(State=#state{inflight=Inflight, seq_num=SeqNum, num_msgs_rcvd=NumMsgsRcvd}, NumTests) ->
    Fmt = "[tests left] ~p [msgs in-flight] ~p [msgs sent] ~p [msgs received] ~p",
    lager:info(Fmt, [NumTests, length(Inflight), SeqNum-1, NumMsgsRcvd]),
    Draw = random:uniform(),
    if
        Draw < 0.4 ->
            TestResult = send(State);
        Draw < 0.8 ->
            TestResult = rcv(State);
        true ->
            TestResult = recover(State)
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
    % generate random payload of random size < 10 bytes
    Msg = crypto:strong_rand_bytes(random:uniform(10)), 
    case molderl:send_message(State#state.stream#stream.pid, Msg) of
        ok ->
            SeqNum = State#state.seq_num,
            Outcome = io_lib:format("[SUCCESS] Sent packet seq num: ~p, msg: ~p", [SeqNum, Msg]),
            Sent = [{SeqNum, Msg}|State#state.sent],
            Inflight = [{SeqNum, Msg}|State#state.inflight],
            {passed, Outcome, State#state{sent=Sent, inflight=Inflight, seq_num=SeqNum+1}};
        _ ->
            Fmt = "[FAILURE] Couldn't send packet seq num: ~p, msg: ~p",
            Reason = io_lib:format(Fmt, [State#state.seq_num, Msg]),
            {failed, Reason}
    end.

recover(State=#state{num_msgs_rcvd=0}) ->
    {passed, "[SUCCESS] No packets were received yet, hence not trying to recover", State};
recover(State=#state{stream=Stream}) ->

    % first, craft and send recovery request
    Start = random:uniform(State#state.num_msgs_rcvd),
    % limit number of requested messages to 40 so as to never bust MTU
    Count = min(40, random:uniform(State#state.num_msgs_rcvd-Start+1)),
    SessionName = molderl_utils:gen_streamname(Stream#stream.name),
    Request = <<SessionName/binary, Start:64, Count:16>>,
    gen_udp:send(Stream#stream.socket, Stream#stream.ip, Stream#stream.recovery_port, Request),
    
    % second, pull out of the sent list the packets expected
    % from recovery reply and add them to in-flight set
    lager:info("start: ~p count: ~p", [Start, Count]),
    Requested = lists:sublist(State#state.sent, State#state.seq_num-Start-Count+1, Count),
    Inflight = State#state.inflight ++ Requested, 

    Fmt = "[SUCCESS] sent recovery request for sequence number ~p count ~p",
    {passed, io_lib:format(Fmt, [Start, Count]), State#state{inflight=Inflight}}.

rcv(State=#state{inflight=[], stream=#stream{name=Name, socket=Socket}}) ->
    case receive_messages(Name, Socket, 100) of
        {error, timeout} ->
            Outcome = "[SUCCESS] Received no packets when none were in flight",
            {passed, Outcome, State};
        {ok, Packets} ->
            Fmt = "[FAILURE] Received ~p packets while none were in flight: ~p",
            {failed, io_lib:format(Fmt, [length(Packets)])}
    end;
rcv(State=#state{inflight=Inflight, stream=#stream{name=Name, socket=Socket}}) ->
    case receive_messages(Name, Socket, 100) of
        {error, timeout} ->
            Fmt = "[FAILURE] Received no packet while ~p were in flight",
            {failed, io_lib:format(Fmt, [length(Inflight)])};
        {ok, Packets} ->
            rcv(State, Packets, 0)
    end.

rcv(State=#state{num_msgs_rcvd=NumMsgsRcvd}, [], RcvdMsgs) ->
    Fmt = "[SUCCESS] Received ~p packets that were in flight",
    {passed, io_lib:format(Fmt, [RcvdMsgs]), State#state{num_msgs_rcvd=NumMsgsRcvd+RcvdMsgs}};
rcv(State, [Observed|Packets], RcvdMsgs) ->
    case lists:member(Observed, State#state.inflight) of
        true ->
            Inflight = lists:delete(Observed, State#state.inflight),
            rcv(State#state{inflight=Inflight}, Packets, RcvdMsgs+1);
        false ->
            Fmt = "[FAILURE] Received packet ~p that was not in flight",
            {failed, io_lib:format(Fmt, [Observed])}
    end.

clean_up() ->
    application:stop(molderl).

