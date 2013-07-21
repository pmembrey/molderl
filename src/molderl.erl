-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-module(molderl).
-behaviour(gen_server).
-define(SERVER, ?MODULE).
-export([start_link/0,init/1,handle_call/3]).
-export([create_stream/4,send_message/2]).
-include("molderl.hrl").
 
-record(state, { channels } ).


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).









init(_Args) ->
    State = #state { channels = []},
    {ok,State}.
    



handle_call({create_stream,StreamProcessName,StreamName,Destination,DestinationPort},_From,State) ->
    spawn_link(molderl_stream,init,[StreamProcessName,StreamName,Destination,DestinationPort]),
    {reply,ok,State}.



create_stream(StreamProcessName,StreamName,Destination,DestinationPort) ->
    gen_server:call(?MODULE,{create_stream,StreamProcessName,StreamName,Destination,DestinationPort}).

send_message(StreamProcessName,Message) ->
    StreamProcessName ! {send,Message},
    ok.
