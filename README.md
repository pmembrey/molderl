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

### Simple tests how-to

Start the server: `application:start(molderl).`

Create a MOLD64 stream: `molderl:create_stream(mystream,{239,0,0,1},8888,8889,{192,168,0,1},5000).`

Send a message: `molderl:send_message(mystream,<<"helloworld">>).`

### Unit tests how-to

    $ rebar eunit

### Instrumentation

If the [statsderl](https://github.com/lpgauth/statsderl) app happens to be running, molderl
will populate the following metrics:
| Type | Name |
| ---- | ---- |
| counter | molderl.<stream_name>.packet.sent |
| timer | molderl.<stream name>.packet.latency* |
| counter | molderl.<stream_name>.recovery_request.received |
| timer | molderl.<stream_name>.recovery_request.latency |

* You can supply an Erlang time 3-tuple {MacroSecs, Secs, MicroSecs}
as the third argument to molderl:send/3 if you want latency to be
measured from an earlier time than when a message enters molderl.

