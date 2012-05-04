%%% @author Alex Robson
%%% @copyright appendTo, 2012
%%% @doc
%%%
%%% A consistent hash based on murmurhash3 =:= stupid fast, low collision.
%%%
%%% @end
%%% @license MIT
%%% Created May 4, 2012 by Alex Robson

-module(hasher).

-export([add/2, remove/1, get/1, start_link/1, init/1]).

-define(SERVER, ?MODULE).

-record(state, {factor, tree=gb_tree2:empty(), lookup=dict:new(), nodelist=dict:new()}).

%%===================================================================
%%% API
%%===================================================================

add(Key, Value) ->
	?MODULE!{add, Key, Value}.

get(Key) ->
	?MODULE!{get, Key, self()},
	receive
		{result, Value} -> Value
	after
		100 -> undefined
	end.

remove(Key) ->
	?MODULE!{ remove, Key}.

%%===================================================================
%%% Main
%%===================================================================

start_link(Factor) ->
	Pid = spawn_link(?SERVER, init, [Factor]),
	register(?MODULE, Pid),
	{ok, Pid}.

init(Factor) ->
	loop(#state{factor = Factor}).

loop(State) ->
	receive
		{add, Key, Value} ->
			loop(add_key(Key, Value, State));
		{get, Key, Caller} ->
			Caller!find(Key, State),
			loop(State);
		{remove, Key} ->
			loop(delete_key(Key, State));
		stop ->
			ok
	end.

%%===================================================================
%%% Internal
%%===================================================================

add_key(Key, Value, State) ->
	Tree = State#state.tree,
	Lookup = State#state.lookup,
	Factor = State#state.factor,
	NodeList = State#state.nodelist,

	NewLookup = dict:append(Key, Value, Lookup),
	AliasKeys = [ 
		murmerl:hash_32( string:join([Key, integer_to_list(X)],".") )
		|| X <- lists:seq(1, Factor) 
	],
	NewNodeList = dict:append(Key, AliasKeys, NodeList),
	NewTree = lists:foldl(fun(X, T) -> gb_tree2:enter(X, Key, T) end, Tree, AliasKeys),
	{Count, _} = NewTree,
	io:format("Tree has ~p nodes ~n", [Count]),
	State#state{ tree = NewTree, lookup = NewLookup, nodelist = NewNodeList }.

find(Key, State) ->
	Tree = State#state.tree,
	Lookup = State#state.lookup,

	HashKey = murmerl:hash_32(Key),
	{value, LookupKey} = gb_tree2:closest(HashKey, Tree),
	dict:fetch(LookupKey, Lookup).

delete_key(Key, State) ->
	Tree = State#state.tree,
	Lookup = State#state.lookup,
	NodeList = State#state.nodelist,

	NewLookup = dict:erase(Key, Lookup),
	AliasKeys = dict:fetch(Key, NodeList),
	NewNodeList = dict:erase(Key, NodeList),
	NewTree = lists:foldl(fun(X, T) -> gb_tree2:delete_any(X, T) end, Tree, AliasKeys),
	%% depending on Factor, this could be *a lot* of deletions,
	%% in order to maintain good lookup perf (assuming deletions are relatively rare)
	%% this rebalances the tree vs. hoping for the best.
	Rebalance = gb_tree2:rebalance(NewTree),
	State#state{ tree = Rebalance, lookup = NewLookup, nodelist = NewNodeList }.

