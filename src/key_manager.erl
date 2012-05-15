%%% @author Alex Robson
%%% @copyright 2012
%%% @doc
%%%
%%% Manages a keylist outside of the instance process. If an instance process
%%% craters, this will allow hasher to rebuild the consistent hash so that
%%% consuming applications don't have to be sad about lost data.
%%% 
%%% @end
%%% Licensed under the MIT license - http://www.opensource.org/licenses/mit-license
%%% Created May 9, 2012 by Alex Robson

-module(key_manager).

-behaviour(gen_server).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% API
-export([add_hash/1, add_key/3, get_keys/1, remove_key/2]).

-record(state, {lookup = dict:new()}).

%%===================================================================
%%% API
%%===================================================================

add_hash(HashId) when is_atom(HashId) ->
    gen_server:cast(?MODULE, {add_hash, HashId}).

add_key(Key, Value, HashId) when is_atom(HashId) ->
    gen_server:cast(?MODULE, {add_key, HashId, Key, Value}).

get_keys(HashId) when is_atom(HashId) ->
    gen_server:call(?MODULE, {get_keys, HashId}).

remove_key(Key, HashId) when is_atom(HashId) ->
    gen_server:cast(?MODULE, {remove_key, HashId, Key}).

%%===================================================================
%%% gen_server
%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    io:format("Everyting gwan to be eyere, mon!"),
    {ok, #state{}}.

handle_call({get_keys, HashId}, _From, State) ->
    {reply, get_keys(HashId, State#state.lookup), State};

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({add_key, HashId, Key, Value}, State) ->
    add_key(HashId, Key, Value, State#state.lookup),
    {noreply, State};

handle_cast({remove_key, HashId, Key}, State) ->
    remove_key(HashId, Key, State#state.lookup),
    {noreply, State};

handle_cast({add_hash, HashId}, State) ->
    {noreply, State#state{lookup = add_hash(HashId, State#state.lookup)}};

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

add_hash(HashId, Lookup) ->
    Table = ets:new(HashId, [set, private, {read_concurrency,true}]),
    dict:append(HashId, Table, Lookup).

add_key(HashId, Key, Value, Lookup) ->
    case dict:find(HashId, Lookup) of
        {ok, [Table]} ->
            ets:insert(Table, {Key, Value}),
            ok;
        _ -> ok
    end.

get_keys(HashId, Lookup) ->
    case dict:find(HashId, Lookup) of
        {ok, [Table]} ->
            [{X,Y} || [{X,Y}] <- ets:match(Table, '$1')];
        _ -> []
    end.

remove_key(HashId, Key, Lookup) ->
    case dict:find(HashId, Lookup) of
        {ok, [Table]} ->
            ets:delete(Table, Key),
            ok;
        _ -> ok
    end.