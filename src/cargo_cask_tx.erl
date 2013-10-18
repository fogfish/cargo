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
	start_link/1,
	init/1,
	free/2,
	ioctl/2,
	handle/3
]).

%% internal state
-record(fsm, {
	rq = undefined :: pid(),  %% read-only queue
	wq = undefined :: pid()   %% write-only queue
}).


%%%------------------------------------------------------------------
%%%
%%% Factory
%%%
%%%------------------------------------------------------------------   

start_link(Peer) ->
	kfsm:start_link(?MODULE, [Peer], []).

init([Peer]) ->
	{ok, Reader} = cargo_peer_sup:reader(Peer),
	{ok, Writer} = cargo_peer_sup:writer(Peer),
	{ok, handle, 
		#fsm{
			rq = Reader,
			wq = Writer
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
	% @todo configurable protocol
	IO = cargo_io:init(?CONFIG_IO_FAMILY, S#fsm.wq),
	try
		plib:ack(Tx, {ok, Fun(IO)}),
		{next_state, idle, S}
	catch _Error:Reason ->
		io:format("--> ~p~n", [erlang:get_stacktrace()]),
		plib:ack(Tx, {error, Reason}),
		{next_state, idle, S}
	after 
		cargo_io:free(IO)	
	end.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   
