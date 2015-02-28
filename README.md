molderl
=======

Sending data via multicast allows an application to send a single packet to a
large number of receivers. Unlike TCP where there is a connection to each
receiver and a given message is sent to each receiver individually, multicast
involves sending a single packet that is then received by every application
who cares to listen for it. There are lots of other advantages too but there
is one fairly severe disadvantage - multicast is not be nature a reliable
protocol.

Often this doesn't matter. If you're streaming live video or audio, a few
dropped packets here and there don't matter. If on the other hand you're
sending data that absolutely must arrive (something you'd generally use TCP
for) then you need something a little bit extra. You need reliable multicast.

There are a couple of well known solutions to this problem. The first is PGM
or Pragmatic Multicast. This is one of the more advanced offerings but is
fairly complex to implement. A much simpler approach with similar features is
MOLDUDP64, a protocol designed by the NASDAQ for delivering price feed data.
This protocol is very lightweight and only adds the bare essentials necessary
to get reliable data transmission.

molderl is a simple server implementation that will allow Erlang applications
to send data using this protocol.

### Quick Start

Start the server:

    > application:start(molderl).

Create a MOLD64 stream:

    > {ok, StreamPID} = molderl:create_stream(mystream,{239,0,0,1},8888,8889).

or, using (optional) arguments:

    > Options = [{ipaddresstosendfrom, {192,168,0,1}}, {filename, "/tmp/mystream"}, {timer,500}, {multicast_ttl, 2}].
    > {ok, StreamPID} = molderl:create_stream(mystream,{239,0,0,1},8888,8889,Options).

Send a message:

    > ok = molderl:send_message(StreamPID, <<"helloworld">>).

### Unit tests how-to

    $ rebar eunit

### Randomized tests how to

    $ cd molderl/
    $ cp src/molderl.app.src ebin/molderl.app
    $ erlc -o ebin/ -I deps/*/include/ deps/*/src/*.erl
    $ erlc -pa ebin/ -o ebin/ -I include/ src/*.erl test/*.erl
    $ erl -pa ebin/ -pa deps/*/ebin/ -eval 'molderl_integration_tests:launch(), init:stop().'

Note that given that packet drops is possible with UDP and the tests do not fully implement
MOLDUDP64 recovery mechanics, the tests will fail intermittently without there necessarily
being something wrong.

### Instrumentation

If the [statsderl](https://github.com/lpgauth/statsderl) app happens to be running, molderl
will populate the following metrics:

| Type | Name |
| ---- | ---- |
| counter | molderl.\<stream_name>.packet.sent |
| timer | molderl.\<stream name>.packet.time_in* |
| timer | molderl.\<stream name>.packet.time_out* |
| counter | molderl.\<stream_name>.recovery_request.received |
| timer | molderl.\<stream_name>.recovery_request.latency |

\* You can supply an Erlang time 3-tuple {MacroSecs, Secs, MicroSecs}
as the third argument to molderl:send_message/3 if you want latency to be
measured from an earlier time than when a message enters molderl.

