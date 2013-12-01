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
%%   cask leader
-module(cargo_cask).
-behaviour(kfsm).

-export([
	start_link/3,
	init/1,
	free/2,
	ioctl/2,
	handle/3
	%% api
]).

%% internal server state
-record(srv, {
}).


%%%------------------------------------------------------------------
%%%
%%% Factory
%%%
%%%------------------------------------------------------------------   

start_link(Owner, Name, Cask) ->
	kfsm:start_link(?MODULE, [Owner, Name, Cask], []).

init([Owner, _Name, _Cask]) ->
	_ = erlang:link(Owner),
	{ok, handle, 
		#srv{
		}
	}.

free(_, _) ->
	ok.

ioctl(_, _) ->
	throw(not_supported).

%%%------------------------------------------------------------------
%%%
%%% api
%%%
%%%------------------------------------------------------------------   


%%%------------------------------------------------------------------
%%%
%%% FSM
%%%
%%%------------------------------------------------------------------   

handle(_, _Tx, S) ->
	{next_state, handle, S}.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

link_to_owner(undefined) ->
	ok;
link_to_owner(Pid) ->
	erlang:link(Pid).
