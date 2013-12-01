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
%%   cask supervisor
-module(cargo_cask_sup).
-behaviour(supervisor).

-include("cargo.hrl").

-export([
   start_link/3,
   init/1,
   %% api
   client_api/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 5000, Type, dynamic}).

%% tx pool
-define(QUEUE(Name, X, Y),  [
   {type,      reusable}
  ,{worker,    {cargo_cask_tx, [Name, X]}}
  ,{capacity,  opts:val(capacity, ?CONFIG_TX_CAPACITY, Y)}
  ,{linger,    opts:val(linger,   ?CONFIG_TX_LINGER,   Y)}
]).

%%
%%
start_link(Owner, Name, Opts) ->
   supervisor:start_link(?MODULE, [Owner, Name, hcask:new(Opts), Opts]).
   
init([Owner, Name, Cask, Opts]) ->   
   {ok,
      {
         {one_for_all, 0, 1},
         [
            % cask leader
            ?CHILD(worker, cargo_cask, [Owner, Name, Cask])
            % cask tx pool
           ,?CHILD(supervisor, pq, [Name, ?QUEUE(Name, Cask, Opts)])
         ]
      }
   }.

%%
%%
child(Sup, Id) ->
   erlang:element(2,
      lists:keyfind(Id, 1, supervisor:which_children(Sup))
   ).

%%
%% return pid of client api
client_api(Sup) ->
   {ok, child(Sup, pq)}.


%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   

