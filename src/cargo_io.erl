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
%%   dirty i/o interface, used by transaction handler
-module(cargo_io). 

-include("cargo.hrl").

-export([
	init/2,
	free/1,
	do/2
]).

%%
%% create new dirty tx handler
-spec(init/2 :: (atom(), #cask{}) -> #cask{}).

init(Protocol, #cask{}=S) ->
	S#cask{
		protocol = Protocol
	}.

%%
%% release dirty tx handler
-spec(free/1 :: (#cask{}) -> #cask{}).

free(#cask{}=Cask0) ->
	Cask = release_socket(Cask0),
	Cask#cask{
		protocol = undefined
	}.


%%
%% execute atomic operation, the function wraps 
%% asynchronous i/o communication to sequence of
%% synchronous primitives. Caller process is blocked
%% until operation completed or timeout
%% 
%% @todo: timeout handling for request
-spec(do/2 :: (#cask{}, any()) -> any()).

do(#cask{protocol=Protocol}=Cask, Req) ->
	do_request(
		lease_socket(request_type(Req), Cask), 
		Protocol:request(Req)
	).

do_request(#cask{socket={_, Socket}}=Cask, Req) ->
   % @todo configurable i/o spin timeout
	do_response(Cask, plib:cast(Socket, Req), 2).

%% wait for response is two stage
%% 1. wait for response and release socket after short time (spin timeout)
%% 2. wait for response and abort transaction on timeout
do_response(#cask{socket=undefined}=Cask, Tx, Timeout) ->
	receive
		% requested bucket is not initialized @todo
		% {Tx, {error, nolink}} ->
		{Tx, Rsp} ->
			handle_response(release_socket(Cask), Rsp)
	after Timeout ->
		exit(timeout)
	end;

do_response(#cask{}=Cask, Tx, Timeout) ->
	receive
		% requested bucket is not initialized @todo
		% {Tx, {error, nolink}} ->
		{Tx, Rsp} ->
			handle_response(release_socket(Cask), Rsp)
	after Timeout ->
		do_response(release_socket(Cask), Tx, Timeout)
	end.


handle_response(#cask{protocol=Protocol}, Msg) ->
	% @todo parse response
	Protocol:response(Msg).


%%
%% check request type 
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
-spec(lease_socket/2 :: (reader | writer, #cask{}) -> #cask{}).

lease_socket(reader, #cask{socket=undefined}=S) ->
	{ok, Pid} = pq:lease(S#cask.reader),
	S#cask{
		socket = {reader, Pid}
	};
lease_socket(writer, #cask{socket=undefined}=S) ->
	{ok, Pid} = pq:lease(S#cask.writer),
	S#cask{
		socket = {writer, Pid}
	};
lease_socket(reader, #cask{socket=writer}=Cask) ->
	lease_socket(reader, release_socket(Cask));
lease_socket(writer, #cask{socket=reader}=Cask) ->
	lease_socket(writer, release_socket(Cask));
lease_socket(_, #cask{}=S) ->
	S.

%%
%% release i/o socket
-spec(release_socket/1 :: (#cask{}) -> #cask{}).

release_socket(#cask{socket=undefined}=S) ->
	S;
release_socket(#cask{socket={reader, Pid}}=S) ->
	pq:release(S#cask.reader, Pid),
	S#cask{
		socket = undefined
	};
release_socket(#cask{socket={writer, Pid}}=S) ->
	pq:release(S#cask.writer, Pid),
	S#cask{
		socket = undefined
	}.

