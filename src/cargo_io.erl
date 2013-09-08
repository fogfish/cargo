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
	init/1,
	free/1,
	do/2
]).

%%
%% create new dirty tx handler
-spec(init/1 :: (atom()) -> #iosock{}).

init(Mod) ->
	#iosock{
		mod = Mod
	}.

%%
%% release dirty tx handler
-spec(free/1 :: (#iosock{}) -> ok).

free(#iosock{}=S) ->
	release(S),
	ok.


%%
%% execute atomic operation
%% @todo: timeout handling for request
-spec(do/2 :: (#iosock{}, any()) -> any()).

do(#iosock{mod=Mod}=S, Req) ->
	do_request(lease(S), Mod:prepare(Req)).

do_request(#iosock{}=S, Req) ->
   % @todo configurable i/o spin timeout
	do_response(S, plib:cast(S#iosock.pid, Req), 2).

%% wait for response is two stage
%% 1. wait for response and release socket after short time
%% 2. wait for response and abort transaction on timeout
do_response(#iosock{pid=undefined}=S, Tx, Timeout) ->
	receive
		% requested bucket is not initialized @todo
		% {Tx, {error, nolink}} ->
		{Tx, Rsp} ->
			handle_response(release(S), Rsp)
	after Timeout ->
		exit(timeout)
	end;

do_response(#iosock{}=S, Tx, Timeout) ->
	receive
		% requested bucket is not initialized @todo
		% {Tx, {error, nolink}} ->
		{Tx, Rsp} ->
			handle_response(release(S), Rsp)
	after Timeout ->
		do_response(release(S), Tx, Timeout)
	end.

	
handle_response(#iosock{}, Rsp) ->
	Rsp.



%%
%% lease i/o socket
-spec(lease/1 :: (#iosock{}) -> #iosock{}).

lease(#iosock{pid=undefined, mod=Mod}=S) ->
	% @todo: deq from node i/o pool
	{ok, Pid} = Mod:start_link(),
	S#iosock{
		pid = Pid
	};

lease(#iosock{}=S) ->
	S.

%%
%% release i/o socket
-spec(release/1 :: (#iosock{}) -> #iosock{}).

release(#iosock{pid=undefined}=S) ->
	S;

release(#iosock{pid=_Pid}=S) ->
	% @todo: enq used pid to node i/o pool
	S#iosock{
		pid = undefined
	}.

