%%% @author Alex Robson
%%% @copyright appendTo, 2012
%%% @doc
%%%
%%% @end
%%% Licensed under the MIT license - http://www.opensource.org/licenses/mit-license
%%% Created May 5, 2012 by Alex Robson

-module(hasher_app).

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() ->
	start([], []).

start(_StartType, _StartArgs) ->
    hasher_sup:start_link().

stop(_State) ->
    ok.
