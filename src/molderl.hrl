
% Some handy defines
-define(PACKET_SIZE,1200).
-define(HEARTBEAT,16#0).
-define(END_OF_SESSION,16#FFFF).

% Helper macro for declaring children of supervisor
-define(CHILD(I, Args, Restart, Type), {I, {I, start_link, Args}, Restart, 5000, Type, [I]}).

