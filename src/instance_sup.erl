%%% @author Alex Robson
%%% @copyright appendTo, 2012
%%% @doc
%%%
%%% Supervises hasher processes.
%%%
%%% @end
%%% Licensed under the MIT license - http://www.opensource.org/licenses/mit-license
%%% Created May 4, 2012 by Alex Robson

-module(instance_sup).
-behaviour(supervisor).
-export([start_link/0, init/1, new/2]).

-define(SERVER, ?MODULE).
-define(CHILD, hash).

%%===================================================================
%%% API
%%===================================================================

start_link() ->
	supervisor:start_link({local, ?SERVER}, ?MODULE, []).

new(Hash, Factor) ->
	case process_util:get_pid(["hasher", Hash]) of
		undefined ->
			supervisor:start_child(?SERVER, [Hash, Factor]);
		_Pid ->
			ok
	end.

init([]) ->
	RestartStrategy = simple_one_for_one,
    MaxRestarts = 3,
    MaxSecondsBetweenRestarts = 60,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    Child = create_child_spec(?CHILD, worker, temporary, 2000, []),

    {ok, {SupFlags, [Child]}}.

%%===================================================================
%%% Internal functions
%%===================================================================

create_child_spec(Child, Type, Restart, Shutdown, Args) ->
    {Child, { Child, start_link, Args }, Restart, Shutdown, Type, [Child]}.