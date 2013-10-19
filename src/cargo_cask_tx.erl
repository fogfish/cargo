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


%%%------------------------------------------------------------------
%%%
%%% Factory
%%%
%%%------------------------------------------------------------------   

start_link(Cask) ->
	kfsm:start_link(?MODULE, [Cask], []).

init([Cask]) ->
	{ok, handle, Cask}.

free(_, _) ->
	ok.

ioctl(_, _) ->
	throw(not_supported).


%%%------------------------------------------------------------------
%%%
%%% transaction handler
%%%
%%%------------------------------------------------------------------   

handle(Fun, Tx, Cask) ->
	% @todo configurable protocol
	IO = cargo_io:init(?CONFIG_IO_FAMILY, Cask),
	try
		plib:ack(Tx, {ok, Fun(IO)}),
		{next_state, idle, Cask}
	catch _Error:Reason ->
		io:format("--> ~p~n", [erlang:get_stacktrace()]),
		plib:ack(Tx, {error, Reason}),
		{next_state, idle, Cask}
	after 
		cargo_io:free(IO)	
	end.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   
