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
-include_lib("hcask/include/hcask.hrl").

-export([
	init/2,
	free/1,
	do/2,
	do/3
]).

%%
%% create new dirty tx handler
-spec(init/2 :: (atom(), #hcask{}) -> #hio{}).

init(Protocol, #hcask{}=Cask) ->
	hcask_io:init(Protocol, Cask).

%%
%% release dirty tx handler
-spec(free/1 :: (#hio{}) -> ok).

free(#hio{}=IO) ->
	hcask_io:free(IO).

%%
%% @todo: timeout handling for request
-spec(do/2 :: (any(), #hio{}) -> {ok, any(), #hio{}} | {error, any(), #hio{}}).
-spec(do/3 :: (atom() | pid(), any(), #hio{}) -> {ok, any(), #hio{}} | {error, any(), #hio{}}).

do(Req, #hio{}=IO) ->
	hcask_io:do(Req, IO).	

do(Cask, Req, #hio{}=IO) ->
	hcask_io:do(Req, select_cask(Cask, IO)).

%%
%% select cask handle
-spec(select_cask/2 :: (atom() | pid(), #hio{}) -> #hio{}).

select_cask(Cask, #hio{cask=[#hcask{name=Cask} | _]}=IO) ->
	% cask is selected
	IO;

select_cask(Cask, #hio{}=IO) ->
	case lists:keytake(Cask, #hcask.name, IO#hio.cask) of
		% cask is exists at context
		{value, Head, Tail} -> 
			IO#hio{cask=[Head | Tail]};
		% cask do not exists
		false ->
			?DEBUG("cargo i/o: select cask ~p", [Cask]),
			{ok, {_, Head}} = pq:worker(Cask),
			IO#hio{cask=[Head | IO#hio.cask]}
	end.


