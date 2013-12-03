
-module(molderl).
-behaviour(gen_server).

-export([start_link/0,create_stream/6,send_message/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { channels = [] } ).

% gen_server API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create_stream(StreamProcessName,StreamName,Destination,DestinationPort,IPAddressToSendFrom,Timer) ->
    gen_server:call(?MODULE,{create_stream,StreamProcessName,StreamName,Destination,DestinationPort,IPAddressToSendFrom,Timer}).

send_message(StreamProcessName,Message) ->
    gen_server:cast(?MODULE,{send,StreamProcessName,Message}).

% gen_server's callbacks

init(_Args) ->
    {ok, #state{}}.

handle_call({create_stream,StreamProcessName,StreamName,Destination,DestinationPort,IPAddressToSendFrom,Timer},_From,State) ->
    spawn_link(molderl_stream,init,[StreamProcessName,StreamName,Destination,DestinationPort,IPAddressToSendFrom,Timer]),
    {reply,ok,State}.

handle_cast({send, StreamProcessName, Message}, State) ->
    StreamProcessName ! {send, Message},
    {noreply, State}.

handle_info(Msg, State) ->
    io:format("Unexpected message in module ~p: ~p~n",[?MODULE, Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(normal, _State) ->
    ok.

