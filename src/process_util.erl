%%% @author Alex Robson
%%% @copyright appendTo, 2012
%%% @doc
%%%
%%% Functions to alias / lookup PIDs
%%%
%%% @end
%%% @license MIT
%%% Created February 8, 2012 by Alex Robson

-module(process_util).
-export([format/2, get_pid/1, get_process_id/1, register/1, send/3]).

%%===================================================================
%%% API functions
%%===================================================================

format(Format, Values) ->
	bitstring_to_list(
		iolist_to_binary(
			io_lib:format(Format, Values)
		)
	).

get_pid(Parts) -> whereis(get_process_id(Parts)).

get_process_id(Parts) -> 
	NewParts = lists:map( fun(X) ->
		case X of
			S when is_list(S) -> S;
			I when is_integer(I) -> integer_to_list(I);
			B when is_binary(B) -> binary_to_list(B);
			A when is_atom(A) -> atom_to_list(A);
			_ -> X
		end
	end, Parts),
	list_to_atom(string:join(NewParts, ".")).

register(Parts) ->
	Name = get_process_id(Parts),
	Pid = self(),
	case whereis(Name) of
		undefined ->
			register(Name, Pid);
		Pid ->
			io:format("~p already registered as ~p~n", [Pid, Name]);
		_ ->
			io:format("your supervisor is dumb~n")
	end.

send(Prefix, Id, Message) ->
	Pid = process_util:get_pid([Prefix, Id]),
	case is_pid(Pid) andalso is_process_alive(Pid) of
		true -> Pid ! Message;
		_ -> ok
	end.