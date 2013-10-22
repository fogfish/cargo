%%
%%   Copyright (c) 2012 Dmitry Kolesnikov
%%   All Rights Reserved.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @description
%%   transaction i/o context
-module(cargo_io). 

-include("cargo.hrl").

-export([
	init/3,
	free/1,
	do/3
]).

%%
%% create new dirty tx handler
-spec(init/3 :: (atom(), atom(), #cask{}) -> #cask{}).

init(Protocol, Peer, #cask{}=Cask) ->
   {ok, Reader} = cargo_peer_sup:reader(Peer),
   {ok, Writer} = cargo_peer_sup:writer(Peer),
	#cargo{
		protocol  = Protocol,
		reader    = {pool, Reader},
		writer    = {pool, Writer},
		cask      = [Cask]
	}.

%%
%% release dirty tx handler
-spec(free/1 :: (#cask{}) -> #cask{}).

free(#cargo{}=Tx) ->
	% free reader socket
	case Tx#cargo.reader of
		{pool, _} -> ok;
		Reader    -> plib:send(Reader, free)
	end,
	% free writer socket
	case Tx#cargo.writer of
		{pool, _} -> ok;
		Writer    -> plib:send(Writer, free)
	end,
	ok.
	
%%
%% execute atomic operation, the function wraps 
%% asynchronous i/o communication to sequence of
%% synchronous primitives. Caller process is blocked
%% until operation completed or timeout
%% 
%% @todo: timeout handling for request
-spec(do/3 :: (atom() | pid(), any(), #cargo{}) -> {ok, any(), #cargo{}} | {error, any(), #cargo{}}).

do(Cask, Req, #cargo{}=Tx) ->
	prepare_request(Req, select_cask(Cask, Tx)).

prepare_request(Req, #cargo{protocol=Protocol, cask=[Cask|_]}=Tx0) ->
   {Socket, Tx} = lease_socket(request_type(Req), Tx0),
	do_request(Protocol:request(Req, Cask), Socket, Tx).

do_request(Req, Socket, Tx) ->
	do_response(plib:cast(Socket, Req), 2, Tx).

%% wait for response is two stage
%% 1. wait for response and release socket after short time (spin timeout)
%% 2. wait for response and abort transaction on timeout
% do_response(Req, Timeout, #cargo{socket=undefined}=Tx) ->
% 	receive
% 		% requested bucket is not initialized @todo
% 		% {Tx, {error, nolink}} ->
% 		{Req, Msg} ->
% 			handle_response(Msg, Tx)
% 	after Timeout ->
% 		exit(timeout)
% 	end;

do_response(Req, Timeout, #cargo{}=Tx) ->
	receive
		% requested bucket is not initialized @todo
		% {Tx, {error, nolink}} ->
		{Req, Msg} ->
			handle_response(Msg, Tx)
	after Timeout ->
		do_response(Req, Timeout, Tx)
	end.


handle_response(Msg, #cargo{protocol=Protocol, cask=[Cask|_]}=Tx) ->
	% @todo parse response
	{ok, Protocol:response(Msg, Cask), Tx}.


%%
%% select cask handle
-spec(select_cask/2 :: (atom() | pid(), #cargo{}) -> #cargo{}).

select_cask(Cask, #cargo{cask=[#cask{id=Cask} | _]}=Tx) ->
	% cask is selected
	Tx;

select_cask(Cask, #cargo{}=Tx) ->
	case lists:keytake(Cask, #cask.id, Tx#cargo.cask) of
		% cask is exists at context
		{value, Head, Tail} -> 
			Tx#cargo{cask   = [Head | Tail]};
		% cask do not exists
		false ->
			?DEBUG("cargo i/o: select cask ~p", [Cask]),
			{ok, {_, Head}} = pq:worker(Cask),
			Tx#cargo{cask   = [Head | Tx#cargo.cask]}
	end.


%%
%% check request type for socket selection
-spec(request_type/1 :: (any()) -> reader | writer).

request_type({create, _}) ->
	writer;
request_type({update, _}) ->
	writer;
request_type({delete, _}) ->
	writer;
request_type({lookup, _}) ->
	reader;
request_type(_) ->
	% @todo remove this check, it is made for RnD only
	reader. 

%%
%% lease i/o socket
%% @todo handle lease timeout
-spec(lease_socket/2 :: (reader | writer, #cargo{}) -> #cargo{}).

lease_socket(reader, #cargo{reader={pool, Pool}}=Tx) ->
	{ok, Socket} = pq:lease(Pool),
	{Socket, Tx#cargo{reader = Socket}};
lease_socket(writer, #cargo{writer={pool, Pool}}=Tx) ->
	{ok, Socket} = pq:lease(Pool),
	{Socket, Tx#cargo{writer = Socket}};
lease_socket(reader, #cargo{}=Tx) ->
	{Tx#cargo.reader, Tx};
lease_socket(writer, #cargo{}=Tx) ->
	{Tx#cargo.writer, Tx}.

% %%
% %% release i/o socket
% -spec(release_socket/1 :: (#cargo{}) -> #cargo{}).

% release_socket(#cargo{socket=undefined}=Tx) ->
% 	Tx;
% release_socket(#cargo{socket={reader, Pid}}=Tx) ->
% 	Tx;
% 	%pq:release(Tx#cargo.reader, Pid),
% 	%Tx#cargo{
% 	%	socket = undefined
% 	%};
% release_socket(#cargo{socket={writer, Pid}}=Tx) ->
% 	Tx.
% 	%pq:release(Tx#cargo.writer, Pid),
% 	%Tx#cargo{
% 	%	socket = undefined
% 	%}.

