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
%%   storage peer i/o supervisor
-module(cargo_peer_sup).
-behaviour(supervisor).

-include("cargo.hrl").

-export([
   start_link/2, 
   init/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 5000, Type, dynamic}).

%%
-define(QUEUE(Port, Opts),  [[
	{type,     reusable}
  ,{worker,   {?CONFIG_IO_PROT, [opts:val(host, Opts), Port]}}
  ,{capacity, opts:val(pool, Opts)}
]]).

%%
%%
start_link(Peer, Opts) ->
   supervisor:start_link({local, Peer}, ?MODULE, [Opts]).
   
init([Opts]) ->   
   {ok,
      {
         {one_for_one, 2, 1800},
         [
         	% reader i/o pool
         	?CHILD(supervisor, reader, pq, ?QUEUE(opts:val(reader, Opts), Opts))

         	% writer i/o pool
           ,?CHILD(supervisor, writer, pq, ?QUEUE(opts:val(writer, Opts), Opts))
         ]
      }
   }.

%%
%%


