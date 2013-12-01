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
-include_lib("hcask/include/hcask.hrl").

-export([start/0, start/1]).
-export([
	start_link/1,
	start_link/2,
   % peer interface
   join/2,
   leave/1,
   peers/0,
	% cask interface
	apply/2,
	apply/3,
	apply_/2,
	apply_/3,
   create/1,
   create/2,
   create/3,
   create_/1,
   create_/2,
   create_/3
   % functional object interface
  ,do_create/2
  ,do_create/3
  ,do_lookup/2
  ,do_lookup/3
   % query
  ,q/1
  ,q/2
  ,q/3
  ,eq/2
  ,gt/2
  ,lt/2
  ,ge/2
  ,le/2


   % create/3,

   ,t/0
]).

-type(cask() :: pid() | atom()).

%%
%% start application
start()    -> 
   applib:boot(?MODULE, []).

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
%%     {struct,    atom()} - struct identity
%%     {keylen, integer()} - length of key   (default 1)
%%     {property,[atom()]} - list of properties
%%     {domain,    atom()} - storage domain   
%%     {bucket,    atom()} - storage bucket  (default struct)
%%     {index,     atom()} - storage index   (default 'PRIMARY')
%%     {capacity, integer()} - pool capacity
%%     {linger,   integer()} - pool linger
-spec(start_link/1 :: (list()) -> {ok, pid()} | {error, any()}).
-spec(start_link/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

start_link(Opts) ->
	start_link(undefined, Opts).

start_link(Name, Opts) ->
	case supervisor:start_child(cargo_cask_root_sup, [self(), Name, Opts]) of
		{ok, Pid} -> cargo_cask_sup:client_api(Pid);
		Error     -> Error
	end.


%%%------------------------------------------------------------------
%%%
%%% peer interface (hcask wrapper)
%%%
%%%------------------------------------------------------------------   

%%
%% join storage peer (hcask wrapper)
%%  Options:
%%    {host,  hostname()} - peer hostname or ip address
%%    {reader, integer()} - peer reader i/o port (read-only request)
%%    {writer, integer()} - peer writer i/o port (mixed request)
%%    {pool,   integer()} - capacity of i/o queues
-spec(join/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

join(Peer, Opts) ->
  hcask:join(Peer, Opts).

%%
%% leave storage peer
-spec(leave/1 :: (atom()) -> ok).

leave(Peer) ->
  hcask:leave(Peer).

%%
%% list of peers
-spec(peers/0 :: () -> [atom()]).

peers() ->
  hcask:peers().

%%%------------------------------------------------------------------
%%%
%%% i/o interface
%%%
%%%------------------------------------------------------------------   

%%
%% execute functional object in cask context
-spec(apply/2  :: (cask(), function()) -> {ok, any()} | {error, any()}).
-spec(apply/3  :: (cask(), function(), timeout()) -> {ok, any()} | {error, any()}).
-spec(apply_/2 :: (cask(), function()) -> reference()).
-spec(apply_/3 :: (cask(), function(), true | false) -> {ok, any()} | {error, any()}).

apply(Cask, Fun) ->
   cargo:apply(Cask, Fun, ?CONFIG_TIMEOUT_IO).
apply(Cask, Fun, Timeout) ->
   request(Cask, {apply, Fun}, Timeout).

apply_(Cask, Fun) ->
   cargo:apply_(Cask, Fun, true).
apply_(Cask, Fun, Flags) ->
   request_(Cask, {apply, Fun}, Flags).


%%
%% create a value
-spec(create/1  :: (any()) -> {ok, integer()} | {error, any()}).
-spec(create/2  :: (cask(), any()) -> {ok, integer()} | {error, any()}).
-spec(create/3  :: (cask(), any(), timeout()) -> {ok, integer()} | {error, any()}).
-spec(create_/2 :: (cask(), any()) -> reference()).
-spec(create_/3 :: (cask(), any(), true | false) -> ok | reference()).

create(Entity) ->
   create(erlang:element(1, Entity), Entity).

create(Entity, Timeout)
 when is_tuple(Entity) ->
   create(erlang:element(1, Entity), Entity, Timeout);
create(Cask, Entity) ->
   create(Cask, Entity, ?CONFIG_TIMEOUT_IO).

create(Cask, Entity, Timeout) ->
   request(Cask, {create, Entity}, Timeout).

create_(Entity) ->
   create_(erlang:element(1, Entity), Entity).

create_(Entity, Flags)
 when is_tuple(Entity) ->
   create_(erlang:element(1, Entity), Entity, Flags);   
create_(Cask, Entity) ->
   create_(Cask, Entity, true).

create_(Cask, Entity, Flags) ->
   request_(Cask, {create, Entity}, Flags).


%%%------------------------------------------------------------------
%%%
%%% query interface
%%%
%%%------------------------------------------------------------------   

q(Key)       -> hcask:q(Key). 
q(Key, NorF) -> hcask:q(Key, NorF).
q(Key, F, N) -> hcask:q(Key, F, N).
eq(Key, Val) -> hcask:eq(Key, Val).
gt(Key, Val) -> hcask:gt(Key, Val).
lt(Key, Val) -> hcask:lt(Key, Val).
ge(Key, Val) -> hcask:ge(Key, Val).
le(Key, Val) -> hcask:le(Key, Val).

%%%------------------------------------------------------------------
%%%
%%% tx interface
%%%
%%%------------------------------------------------------------------   

%%
%% create a value
-spec(do_create/2  :: (any(), #hio{}) -> {ok, any(), #hio{}} | {error, any(), #hio{}}).
-spec(do_create/3  :: (cask(), any(), #hio{}) -> {ok, any(), #hio{}} | {error, any(), #hio{}}).

do_create(Entity, Context) ->
   cargo_io:do({create, Entity}, Context).

do_create(Cask, Entity, Context) ->
   cargo_io:do(Cask, {create, Entity}, Context).


%%
%% lookup value(s)
-spec(do_lookup/2  :: (any(), #hio{}) -> {ok, any(), #hio{}} | {error, any(), #hio{}}).
-spec(do_lookup/3  :: (cask(), any(), #hio{}) -> {ok, any(), #hio{}} | {error, any(), #hio{}}).

do_lookup(Query, Context) ->
   cargo_io:do(Query, Context).

do_lookup(Cask, Query, Context) ->
   cargo_io:do(Cask, Query, Context).




%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

%%
%% synchronous request
request(Cask, Req, Timeout) ->
   case pq:lease(Cask, Timeout) of
      {ok, Pid} -> plib:call(Pid, Req, Timeout);
      Error     -> Error
   end.

%%
%% asynchronous request
request_(Cask, Req, true) ->
   case pq:lease(Cask, ?CONFIG_TIMEOUT_LEASE) of
      {ok, Pid} -> plib:cast(Pid, Req);
      Error     -> Error
   end;
request_(Cask, Req, false) ->
   case pq:lease(Cask, ?CONFIG_TIMEOUT_LEASE) of
      {ok, Pid} -> plib:send(Pid, Req), ok;
      Error     -> Error
   end.



%%
%% cask 
-define(KV, [
   {peer,     mysqld}
  ,{struct,   kv}
  ,{property, [key,val]}
  ,{db,       hcask}
]).



t() ->
   _ = cargo:join(mysqld, [{host, mysqld}, {reader, 9998}, {writer, 9999}, {pool, 10}]),
   {ok, Pid} = cargo:start_link(?KV),
   % {ok,   _} = cargo:cask(aaa, [
   %    {peer,     test}
   %   ,{struct,   test}
   %   ,{property, [a,b,c,d,e,f]}
   %   ,{domain,   test}
   % ]),

   lager:set_loglevel(lager_console_backend, debug),
   cargo:apply(Pid, fun(X) -> tx(Pid, X) end).


tx(Pid, IO) ->
   Q = cargo:q(cargo:eq(key, 1)),
   case cargo:do_lookup(Q, IO) of
      {ok, [], IO1} ->
         cargo:do_create({kv, 1, "abc"}, IO1);
      Result ->
         Result
   end.

   %{ok, ok, IO}.
	% {ok, Result, Cask1} = cargo:do_create(Pid, {a,b,c}, Cask0), 
 %   %cargo:create(IO, {a,b,c}),
	% io:format("--> ~p~n", [Result]),
	% {ok, _, Cask2} = cargo:do_create(aaa, req1, Cask1).


