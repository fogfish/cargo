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
%%   i/o protocol handler - handler socket
-module(cargo_io_hs).
-behaviour(pipe).

-include("cargo.hrl").

-export([
	start_link/3,
	init/1,
	free/2,
	ioctl/2,
	io/3,

	request/2,
	response/2
]).

%% internal state
-record(fsm, {
	queue   = undefine :: pid(),    %% queue leader i/o object belongs to
	spinner = undefine :: any(),    %% spinner timeout
	q       = undefine :: datum:q() %% on-going tx
}).

%%%------------------------------------------------------------------
%%%
%%% Factory
%%%
%%%------------------------------------------------------------------   

start_link(Queue, Host, Port) ->
	pipe:start_link(?MODULE, [Queue, Host, Port], []).

init([Queue, _Host, _Port]) ->
   {ok, Pid} = pq:queue(Queue),
	{ok, io, 
		#fsm{
			queue   = Pid,
			spinner = opts:val(spin, 10, cargo),
			q       = deq:new()
		}
	}.

free(_, _) ->
	ok.

ioctl(_, _) ->
	throw(not_supported).
	
%%%------------------------------------------------------------------
%%%
%%% i/o handler
%%%
%%%------------------------------------------------------------------   

io(timeout, _Tx, S) ->
	%% @todo make busy (leased) / free (released) state (double release NFG)
	?DEBUG("cargo hs: ~p spin expired", [self()]),
	pq:release(S#fsm.queue, self()),
	{next_state, io, S};

io(free, _Tx, S) ->
	?DEBUG("cargo hs: ~p free", [self()]),
	pq:release(S#fsm.queue, self()),
	{next_state, io, 
		S#fsm{
			spinner = tempus:cancel(S#fsm.spinner)
		}
	};

io({tcp, _, Msg}, _, S) ->
	{Tx, Q} = q:deq(S#fsm.q),
	plib:ack(Tx, Msg),
	{next_state, io,
		S#fsm{
			q = Q
		}
	};

io(Msg, Tx, S) ->
	%% @todo socket i/o
	erlang:send_after(1, self(), {tcp, undefined, Msg}),
	{next_state, io, 
		S#fsm{
			spinner = tempus:reset(S#fsm.spinner, timeout),
			q       = q:enq(Tx, S#fsm.q)
		}
	}.


%%
%% serialize client request to protocol format
%%  * resolves physical bucket
%%  * resolves physical bucket handle (32-bit number)
%%  * splits tuple to key / val parts
%%  * serializes request to write format 
request(Req, _Cask) ->
	Req.

%%
%% serializes storage response to client tuple
response(Rsp, _Cask) ->
	Rsp.


