
% Some handy defines
-define(PACKET_SIZE,1200).
-define(HEARTBEAT,16#0).
-define(END_OF_SESSION,16#FFFF).

% Helper macro for declaring children of supervisor
-define(CHILD(ID, MODULE, Args, Restart, Type),
        {ID, {MODULE, start_link, Args}, Restart, 5000, Type, [MODULE]}).

