%%% @author Alex Robson
%%% @copyright 2012
%%% @doc
%%%
%%% 
%%% 
%%% @end
%%% Licensed under the MIT license - http://www.opensource.org/licenses/mit-license
%%% Created , 2012 by Alex Robson

-module(hash).

-behaviour(gen_server).

-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(PREFIX, "hasher").

-record(state, {id, nodes, tree=gb_tree2:empty(), lookup=dict:new(), nodelist=dict:new()}).

%%===================================================================
%%% gen_server
%%===================================================================

start_link(HashId, VirtualNodes) ->
    gen_server:start_link(?MODULE, [HashId, VirtualNodes], []).

init([HashId, VirtualNodes]) ->
    State = #state{id = HashId, nodes = VirtualNodes},
    process_util:register([?PREFIX, HashId]),
    NewState =
        case key_manager:get_keys(HashId) of
            [] -> 
                key_manager:add_hash(HashId),
                State;
            List -> lists:foldl( fun({K, V}, S) -> add_key(K, V, S) end, State, List)
        end,
    {ok, NewState}.

handle_call(count, _From, State) ->
    {Count, _} = State#state.tree,
    {reply, Count, State};

handle_call({get, Key}, _From, State) ->
    {reply, find(Key, State), State};

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({add, Key, Value}, State) ->
    {noreply, add_key(Key, Value, State)};

handle_cast({remove, Key}, State) ->
    {noreply, delete_key(Key, State)};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%===================================================================
%%% Internal
%%===================================================================

add_key(Key, Value, State) when is_binary(Key) ->
    Tree = State#state.tree,
    Lookup = State#state.lookup,
    VirtualNodes = State#state.nodes,
    NodeList = State#state.nodelist,
    HashId = State#state.id,
    key_manager:add_key(Key, Value, HashId),

    NewLookup = dict:append(Key, Value, Lookup),
    BitList = binary_to_list(Key),
    AliasKeys = [ 
        murmerl:hash_32( lists:append(BitList, [X]) )
        || X <- lists:seq(1, VirtualNodes) 
    ],
    NewNodeList = dict:append(Key, AliasKeys, NodeList),
    NewTree = lists:foldl(fun(X, T) -> gb_tree2:enter(X, Key, T) end, Tree, AliasKeys),
    %% depending on VirtualNodes, this could be *a lot* of insertions,
    %% without a balanced tree, lookups will take longer and
    %% distribution across the key space will become warped.
    Rebalance = gb_tree2:balance(NewTree),
    State#state{ tree = Rebalance, lookup = NewLookup, nodelist = NewNodeList };

add_key(Key, Value, State) ->
    add_key(term_to_binary(Key), Value, State).

find(Key, State) ->
    Tree = State#state.tree,
    Lookup = State#state.lookup,

    HashKey = murmerl:hash_32(Key),
    {value, LookupKey} = gb_tree2:closest(HashKey, Tree),
    case dict:is_key(LookupKey, Lookup) of
        true -> dict:fetch(LookupKey, Lookup);
        _ -> not_found
    end.

delete_key(Key, State) when is_binary(Key) ->
    Tree = State#state.tree,
    Lookup = State#state.lookup,
    NodeList = State#state.nodelist,
    HashId = State#state.id,
    key_manager:remove_key(Key, HashId),

    case dict:is_key(Key, Lookup) of
        true ->
            NewLookup = dict:erase(Key, Lookup),
            AliasKeys = lists:flatten(dict:fetch(Key, NodeList)),
            NewNodeList = dict:erase(Key, NodeList),
            NewTree = case AliasKeys of
                [] -> Tree;
                Keys ->
                    T2 = lists:foldl(fun(X, T) -> gb_tree2:delete_any(X, T) end, Tree, Keys),
                    %% depending on VirtualNodes, this could be *a lot* of deletions,
                    %% without a balanced tree, lookups will take longer and
                    %% distribution across the key space will become warped.
                    gb_tree2:balance(T2)
            end,
            State#state{ tree = NewTree, lookup = NewLookup, nodelist = NewNodeList };
        _ -> State
    end;

delete_key(Key, State) ->
    delete_key(term_to_binary(Key), State).