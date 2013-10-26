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
%%   module implements client transaction as unit-of-work w/o any rollback 
%%   feature. It executes a functional object within single i/o context.
%%   The functional object consists of a series of synchronous bucket i/o 
%%   requests. The object execution is aborted if something goes wrong 
%%   such as network error, storage failure, etc. The transaction returns
%%   either {ok, any()} | {error, any()}. 
%%
-module(cargo_cask_tx).
-behaviour(kfsm).

-include("cargo.hrl").

-export([
	start_link/2,
	init/1,
	free/2,
	ioctl/2,
	handle/3
]).

%% internal tx state
-record(srv, {
	queue   = undefined :: pid(),
	context = undefined :: #cargo{}

}).

%%%------------------------------------------------------------------
%%%
%%% Factory
%%%
%%%------------------------------------------------------------------   

start_link(Queue, Cask) ->
	kfsm:start_link(?MODULE, [Queue, Cask], []).

init([Queue, Cask]) ->
   % @todo configurable protocol
	Context = cargo_io:init(?CONFIG_IO_FAMILY, Cask#cask.peer, set_cask_id(Queue, Cask)),
	{ok, handle, 
		#srv{
			queue   = Queue,
			context = Context
		}
	}.

free(_, _) ->
	ok.

ioctl(_, _) ->
	throw(not_supported).


%%%------------------------------------------------------------------
%%%
%%% transaction handler
%%%
%%%------------------------------------------------------------------   

handle(Fun, Tx, S) ->
	try
		{Status, Result, Context} = Fun(S#srv.context),
		plib:ack(Tx, {Status, Result}),
		cargo_io:free(Context),
		pq:release(S#srv.queue, self()),
		{next_state, idle, S}
	catch _Error:Reason ->
		?ERROR("cargo tx failed: ~p ~p", [Reason, erlang:get_stacktrace()]),
		plib:ack(Tx, {error, Reason}),
		% no need to clean-up context i/o handlers release itself after spin i/o
		pq:release(S#srv.queue, self()),
		{next_state, idle, S}
	end.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

%%
set_cask_id(Pid,  #cask{id=undefined}=S) ->
	S#cask{id=Pid};
set_cask_id(_Pid, Cask) ->
	Cask.

