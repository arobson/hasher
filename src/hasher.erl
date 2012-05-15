%%% @author Alex Robson
%%% @copyright 2012
%%% @doc
%%%
%%% 
%%% 
%%% @end
%%% Licensed under the MIT license - http://www.opensource.org/licenses/mit-license
%%% Created May 12, 2012 by Alex Robson

-module(hasher).

-export([start/0, stop/0]).
%% api calls
-export([add/3, remove/2, get/2, new/1, new/2, node_count/1]).

-define(PREFIX, "hasher").

%%===================================================================
%%% Spec
%%===================================================================

-spec add( any(), any(), any() ) -> ok.

-spec get( any(), any() ) -> any() | undefined.

-spec new( any() ) -> ok.
-spec new( any(), pos_integer() ) -> ok.

-spec node_count( any() ) -> integer | unknown.

-spec remove( any(), any() ) -> ok.

%%===================================================================
%%% API
%%===================================================================

add(Key, Value, HashId) ->
    cast(HashId, {add, Key, Value}),
    ok.

get(Key, HashId) ->
    call(HashId, {get, Key}).

new(HashId) ->
    new(HashId, 100), 
    ok.

new(HashId, VirtualNodes) ->
    instance_sup:new(HashId, VirtualNodes),
    ok.

node_count(HashId) ->
    call(HashId, count).

remove(Key, HashId) ->
    cast(HashId, {remove, Key}).

start() ->
	application:load(hasher),
	application:start(hasher).

stop() ->
	application:stop(hasher).

%%===================================================================
%%% Internal
%%===================================================================

call(HashId, Message) ->
    case process_util:get_pid([?PREFIX, HashId]) of
    	undefined -> no_hash;
    	Pid -> gen_server:call(Pid, Message)
	end.

cast(HashId, Message) ->
    case process_util:get_pid([?PREFIX, HashId]) of
    	undefined -> no_hash;
    	Pid -> gen_server:cast(Pid, Message)
	end.