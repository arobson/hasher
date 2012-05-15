%%% @author Alex Robson
%%% @copyright appendTo, 2012
%%% @doc
%%%
%%% App level supervisor
%%%
%%% @end
%%% Licensed under the MIT license - http://www.opensource.org/licenses/mit-license
%%% Created May 9, 2012 by Alex Robson

-module(hasher_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

-define(SERVER, ?MODULE).

%%===================================================================
%%% API
%%===================================================================

start_link() ->
	supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
	RestartStrategy = one_for_one,
    MaxRestarts = 3,
    MaxSecondsBetweenRestarts = 60,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    KeyManager = create_child_spec(key_manager, worker, permanent, 1000, []),
    InstanceSup = create_child_spec(instance_sup, supervisor, permanent, 2000, []),
    
    {ok, {SupFlags, [KeyManager, InstanceSup]}}.

%%===================================================================
%%% Internal functions
%%===================================================================

create_child_spec(Child, Type, Restart, Shutdown, Args) ->
    {Child, { Child, start_link, Args }, Restart, Shutdown, Type, [Child]}.