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

-export([
	start_link/2,
	init/1,
	free/2,
	ioctl/2,
	io/3,

	request/1,
	response/1
]).

%% internal state
-record(fsm, {
}).

%%%------------------------------------------------------------------
%%%
%%% Factory
%%%
%%%------------------------------------------------------------------   

start_link(Host, Port) ->
	pipe:start_link(?MODULE, [Host, Port], []).

init([_Host, _Port]) ->
	{ok, io, 
		#fsm{
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

io(Msg, Tx, S) ->
	plib:ack(Tx, Msg),
	{next_state, io, S}.


%%
%% serialize client request to protocol format
%%  * resolves physical bucket
%%  * resolves physical bucket handle (32-bit number)
%%  * splits tuple to key / val parts
%%  * serializes request to write format 
request(Req) ->
	Req.

%%
%% serializes storage response to client tuple
response(Rsp) ->
	Rsp.


