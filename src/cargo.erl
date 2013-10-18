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
%%
-module(cargo).

-include("cargo.hrl").

-export([start/0, start/1]).
-export([
	start_link/1,
	start_link/2,
	cask/1,
	% cask interface

	% peer interface
	join/2,
	leave/1,
	peers/0,

	do/2,

	t/0
]).

%%
%% start application
start()    -> 
	start(filename:join(["./priv", "dev.config"])).

start(Cfg) -> 
	applib:boot(?MODULE, Cfg).

%%%------------------------------------------------------------------
%%%
%%% cask interface
%%%
%%%------------------------------------------------------------------   

%%
%% start storage cask (transaction pool), returns supervisor tree
%%
%%  Options:
%%     {peer,        atom()} - peer name
%%     {capacity, integer()} - pool capacity
%%     {linger,   integer()} - pool linger
-spec(start_link/1 :: (list()) -> {ok, pid()} | {error, any()}).
-spec(start_link/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

start_link(Opts) ->
   cargo_cask_sup:start_link(Opts).

start_link(Name, Opts) ->
   cargo_cask_sup:start_link(Name, Opts).

%%
%% start storage cask components, returns pid of cask process
%%
%%  Options:
%%     {peer,        atom()} - peer name
%%     {capacity, integer()} -
%%     {linger,   integer()} -
-spec(cask/1 :: (list()) -> {ok, pid()} | {error, any()}).

cask(Opts)
 when is_list(Opts) ->
   case start_link(Opts) of
      {ok, Pid} -> cargo_cask_sup:client_api(Pid);
      Error     -> Error
   end;

cask(Pid)
 when is_pid(Pid) ->
   cargo_cask_sup:client_api(Pid).


%%%------------------------------------------------------------------
%%%
%%% peer interface
%%%
%%%------------------------------------------------------------------   

%%
%% join storage peer
%%  Options:
%%    {host,  hostname()} - peer hostname or ip address
%%    {reader, integer()} - peer reader i/o port (read-only request)
%%    {writer, integer()} - peer writer i/o port (mixed request)
%%    {pool,   integer()} - capacity of i/o queues
-spec(join/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

join(Peer, Opts) ->
	cargo_sup:join(Peer, Opts).

%%
%% leave storage peer
-spec(leave/1 :: (atom()) -> ok).

leave(Peer) ->
	cargo_sup:leave(Peer).

%%
%% list of peers
-spec(peers/0 :: () -> [atom()]).

peers() ->
	cargo_sup:peers().

%%%------------------------------------------------------------------
%%%
%%% i/o interface
%%%
%%%------------------------------------------------------------------   

%%
%% execute dirty operation over i/o socket, current process is blocked 
-spec(do/2 :: (any(), any()) -> {ok, any()} | {error, any()}).

do(Sock, Req) ->
	cargo_io:do(Sock, Req).

%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

%%     {struct,    atom()} - struct identity
%%     {keylen, integer()} - length of key   (default 1)
%%     {property,[atom()]} - list of properties
%%     {domain,    atom()} - storage domain   
%%     {bucket,    atom()} - storage bucket  (default struct)
%%     {index,     atom()} - storage index   (default 'PRIMARY')

%%
%% build cask meta data from options 
define_cask(Opts) ->
   define_cask(Opts, #cask{}).
define_cask([{peer, X} | Opts], S) ->
   define_cask(Opts, S#cask{peer=X});
define_cask([{struct, X} | Opts], S) ->
   define_cask(Opts, S#cask{struct=X});
define_cask([{keylen, X} | Opts], S) ->
   define_cask(Opts, S#cask{keylen=X});
define_cask([{property, X} | Opts], S) ->
   define_cask(Opts, S#cask{property=X});
define_cask([{domain, X} | Opts], S) ->
   define_cask(Opts, S#cask{domain=X});
define_cask([{bucket, X} | Opts], S) ->
   define_cask(Opts, S#cask{bucket=X});
define_cask([{index, X} | Opts], S) ->
   define_cask(Opts, S#cask{index=X});
define_cask([{capacity, X} | Opts], S) ->
   define_cask(Opts, S#cask{capacity=X});
define_cask([{linger, X} | Opts], S) ->
   define_cask(Opts, S#cask{linger=X});
define_cask([_ | Opts], S) ->
   define_cask(Opts, S);
define_cask([], Cask) ->
   Cask.

assert_cask(#cask{peer=undefined}) ->
   exit({bagarg, peer});
assert_cask(#cask{struct=undefined}) ->
   exit({bagarg, struct});
assert_cask(#cask{property=undefined}) ->
   exit({bagarg, property});
assert_cask(#cask{domain=undefined}) ->
   exit({bagarg, domain});
assert_cask(#cask{bucket=undefined}=S) ->
   assert_cask(S#cask{bucket=S#cask.struct});
assert_cask(#cask{index=undefined}=S) ->
   assert_cask(S#cask{index=?CONFIG_INDEX});
assert_cask(Cask) ->
   Cask.






t() ->
   {ok,   _} = cargo:join(test, [{host, mysqld}, {reader, 80}, {writer, 80}, {pool, 10}]),
   {ok, Pid} = cargo:cask([
      {peer,     test}
     % ,{struct,   test}
     % ,{property, [a,b,c]}
     % ,{domain,   test}
   ]),
   {ok,  Tx} = pq:lease(Pid),
	plib:call(Tx, fun tx/1).


tx(IO) ->
	R = cargo:do(IO, req),
	io:format("--> ~p~n", [R]),
	cargo:do(IO, req1).


