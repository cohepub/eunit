%% This library is free software; you can redistribute it and/or modify
%% it under the terms of the GNU Lesser General Public License as
%% published by the Free Software Foundation; either version 2 of the
%% License, or (at your option) any later version.
%%
%% This library is distributed in the hope that it will be useful, but
%% WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
%% Lesser General Public License for more details.
%%
%% You should have received a copy of the GNU Lesser General Public
%% License along with this library; if not, write to the Free Software
%% Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
%% USA
%%
%% $Id$ 
%%
%% @private (for now)
%% @author Richard Carlsson <richardc@it.uu.se>
%% @copyright 2006 Richard Carlsson
%% @doc Erlang file monitoring service

-module(file_monitor).

-behaviour(gen_server).

-export([monitor_file/2, monitor_file/3, monitor_dir/2, monitor_dir/3,
	 demonitor/1, demonitor/2, get_poll_time/0, get_poll_time/1,
	 set_poll_time/1, set_poll_time/2]).

-export([start/0, start/1, start/2, start_link/0, start_link/1,
	 start_link/2, stop/0, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 code_change/3, terminate/2]).

-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").


%% The behaviour of this service is inspired by the open source FAM
%% daemon [http://oss.sgi.com/projects/fam/].
%%
%% NOTE: Monitored paths should be absolute, but this is not checked.
%% 
%% We never change the paths, e.g. from relative to absolute, but we
%% make sure that the path is a flat string, for the sake of
%% comparisons, and return this to the caller for reference.


-define(DEFAULT_POLL_TIME, 5000).  % change with option poll_time
-define(MIN_POLL_TIME, 100).
-define(SERVER, ?MODULE).
-define(MSGTAG, ?SERVER).

-record(state, {poll_time,  % polling interval (milliseconds)
		dirs,       % map: directory path -> entry
		files,      % map: file path -> entry
		refs,       % map: monitor Ref -> {Pid,set(Object)}
		clients     % map: client Pid -> erlang:monitor/2 Ref
	       }).

-record(monitor, {pid, reference}).

-record(entry, {info = undefined, files = [], monitors = sets:new()}).


%%
%% User interface
%%

monitor_file(Path, Pid) ->
    monitor_file(?SERVER, Path, Pid).

monitor_file(Server, Path, Pid) ->
    monitor(Server, file, Path, Pid).


monitor_dir(Path, Pid) ->
    monitor_dir(?SERVER, Path, Pid).

monitor_dir(Server, Path, Pid) ->
    monitor(Server, directory, Path, Pid).


monitor(Server, Type, Path, Pid) when is_pid(Pid) ->
    FlatPath = filename:flatten(Path),
    Cmd = {monitor, {Type, FlatPath}, Pid},
    {ok, Ref} = gen_server:call(Server, Cmd),
    {ok, FlatPath, Ref}.


demonitor(Ref) ->
    demonitor(?SERVER, Ref).

demonitor(Server, Ref) ->
    ok = gen_server:call(Server, {demonitor, Ref}).


get_poll_time() ->
    get_poll_time(?SERVER).

get_poll_time(Server) ->
    gen_server:call(Server, get_poll_time).


set_poll_time(Time) ->
    set_poll_time(?SERVER, Time).

set_poll_time(Server, Time) ->
    gen_server:call(Server, {set_poll_time, Time}).
    


start() ->
    start([]).

start(Options) ->
    start({local, ?SERVER}, Options).

start(undefined, Options) ->
    gen_server:start(?MODULE, Options, []);
start(Name, Options) ->
    gen_server:start(Name, ?MODULE, Options, []).

start_link() ->
    start_link([]).

start_link(Options) ->
    start_link({local, ?SERVER}, Options).

start_link(undefined, Options) ->
    gen_server:start_link(?MODULE, Options, []);
start_link(Name, Options) ->
    gen_server:start_link(Name, ?MODULE, Options, []).


stop() ->
    stop(?SERVER).

stop(Server) ->
    gen_server:call(Server, stop),
    ok.


%%
%% gen_server callbacks
%%

init(Options) ->
    Time = safe_poll_time(proplists:get_value(poll_time, Options)),
    St = #state{poll_time = Time,
		dirs = dict:new(),
		files = dict:new(),
		clients = dict:new(),
		refs = dict:new()},
    set_timer(St),
    {ok, St}.


handle_call({monitor, Object, Pid}, _From, St) when is_pid(Pid) ->
    {Ref, St1} = new_monitor(Object, Pid, St),
    {reply, {ok, Ref}, add_client(Pid, St1)};
handle_call({demonitor, Ref}, _From, St) when is_reference(Ref) ->
    {reply, ok, delete_monitor(Ref, St)};
handle_call(get_poll_time, _From, St) ->
    {reply, St#state.poll_time, St};
handle_call({set_poll_time, Time}, _From, St) ->
    {reply, ok, St#state{poll_time=safe_poll_time(Time)}};
handle_call(stop, _From, St) ->
    {stop, normal, ok, St}.

handle_cast(_, St) ->
    {noreply, St}.

handle_info(poll_time, St) ->  
    {noreply, set_timer(poll(St))};
handle_info({'DOWN', _Ref, process, Pid, _Info}, St) ->
    {noreply, del_client(Pid, St)};
handle_info(_, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, _St) ->
    ok.

%%
%% Internal functions
%%

max(X, Y) when X > Y -> X;
max(_, Y) -> Y.

min(X, Y) when X < Y -> X;
min(_, Y) -> Y.

safe_poll_time(N) when is_integer(N) ->
    min(16#FFFFffff, max(N, ?MIN_POLL_TIME));
safe_poll_time(_) -> ?DEFAULT_POLL_TIME.

set_timer(St) ->
    erlang:send_after(St#state.poll_time, self(), poll_time),
    St.


%% client monitoring (once a client, always a client - until death)

add_client(Pid, St) ->
    case dict:is_key(Pid, St#state.clients) of
	true ->
	    St;
	false ->
	    Ref = erlang:monitor(process, Pid),
	    St#state{clients = dict:store(Pid, Ref, St#state.clients)}
    end.

del_client(Pid, St0) ->
    St = purge_pid(Pid, St0),
    case dict:find(Pid, St#state.clients) of
	{ok, Ref} ->
	    erlang:demonitor(Ref, [flush]),
	    St#state{clients = dict:erase(Pid, St#state.clients)};
	error ->
	    St
    end.

%% Adding a new monitor

new_monitor(Object, Pid, St) ->
    Ref = make_ref(),
    new_monitor(Object, Pid, Ref, St).

new_monitor(Object, Pid, Ref, St) ->
    Objects = case dict:find(Ref, St#state.refs) of
		  {ok, {_Pid, Set}} -> Set;
		  error -> sets:new()
	      end,
    Refs = dict:store(Ref, {Pid, sets:add_element(Object, Objects)},
		      St#state.refs),
    Monitor = #monitor{pid = Pid, reference = Ref},
    {Ref, monitor_path(Object, Monitor, St#state{refs = Refs})}.

%% We must separate the namespaces for files and dirs; there may be
%% simultaneous file and directory monitors for the same path, and a
%% file may be deleted and replaced by a directory of the same name, or
%% vice versa. The client should know (more or less) if a path is
%% expected to refer to a file or a directory.

monitor_path({file, Path}, Monitor, St) ->
    St#state{files = monitor_path(Path, Monitor, file, St#state.files)};
monitor_path({directory, Path}, Monitor, St) ->
    St#state{dirs = monitor_path(Path, Monitor, directory, St#state.dirs)}.

%% Adding a new monitor forces an immediate poll of the path, such that
%% previous monitors only see any real change, while the new monitor
%% either gets {exists, ...} or {error, ...}.

monitor_path(Path, Monitor, Type, Dict) ->
    Entry = case dict:find(Path, Dict) of
		{ok, OldEntry} -> poll_file(Path, OldEntry, Type);
		error -> new_entry(Path, Type)
	    end,
    event(#entry{}, dummy_entry(Entry, Monitor), Type, Path),
    NewEntry = Entry#entry{monitors =
			   sets:add_element(Monitor,
					    Entry#entry.monitors)},
    dict:store(Path, NewEntry, Dict).

dummy_entry(Entry, Monitor) ->
    Entry#entry{monitors = sets:add_element(Monitor, sets:new())}.

new_entry(Path, Type) ->
    refresh_entry(Path, #entry{monitors = sets:new()}, Type).

%% deleting a monitor by reference

delete_monitor(Ref, St) ->
    case dict:find(Ref, St#state.refs) of
	{ok, {_Pid, Objects}} ->
	    sets:fold(fun (Object, St0) ->
			      demonitor_path(Ref, Object, St0)
		      end,
		      St#state{refs = dict:erase(Ref, St#state.refs)},
		      Objects);
	error ->
	    St
    end.

demonitor_path(Ref, {file, Path}, St) ->
    St#state{files = demonitor_path_1(Path, Ref, St#state.files)};
demonitor_path(Ref, {directory, Path}, St) ->
    St#state{dirs = demonitor_path_1(Path, Ref, St#state.dirs)}.

demonitor_path_1(Path, Ref, Dict) ->
    case dict:find(Path, Dict) of
	{ok, Entry} -> 
	    purge_empty_sets(
	      dict:store(Path, purge_monitor_ref(Ref, Entry), Dict));
	error ->
	    Dict
    end.

purge_monitor_ref(Ref, Entry) ->
    Entry#entry{monitors =
		sets:filter(fun (#monitor{reference = R})
				when R =:= Ref -> false;
				(_) -> true
			    end,
			    Entry#entry.monitors)}.

%% purging monitoring information related to a deleted client Pid
%% TODO: this should use the refs mapping to avoid visiting all paths

purge_pid(Pid, St) ->
    Files = dict:map(fun (_Path, Entry) ->
			     purge_monitor_pid(Pid, Entry)
		     end,
		     St#state.files),
    Dirs = dict:map(fun (_Path, Entry) ->
			    purge_monitor_pid(Pid, Entry)
		    end,
		    St#state.dirs),
    Refs = dict:filter(fun (_Ref, {P, _})
			   when P =:= Pid -> false;
			   (_, _) -> true
		       end,
		       St#state.refs),
    St#state{refs = Refs,
	     files = purge_empty_sets(Files),
	     dirs = purge_empty_sets(Dirs)}.

purge_monitor_pid(Pid, Entry) ->
    Entry#entry{monitors =
		sets:filter(fun (#monitor{pid = P})
				when P =:= Pid -> false;
				(_) -> true
			    end,
			    Entry#entry.monitors)}.

purge_empty_sets(Dict) ->
    dict:filter(fun (_Path, Entry) ->
			sets:size(Entry#entry.monitors) > 0
		end, Dict).


%% FIXME: clarify role of Type below: it is the monitor Type, not actual file type (see the file_info record for that)

%% Generating events upon state changes by comparing old and new states
%% 
%% Message formats:
%%   {exists, Path, Type, #file_info{}, Files}
%%   {changed, Path, Type, #file_info{}, Files}
%%   {error, Path, Type, Info}
%%
%% Type is file or directory, as specified by the monitor type, not by
%% the actual type on disk. If Type is file, Files is always []. If Type
%% is directory, Files is a list of {added, FileName} and {deleted,
%% FileName}, where FileName is on basename form, i.e., without any
%% directory component.
%%
%% When a new monitor is installed for a path, an initial {exists,...}
%% or {error,...} message will be sent to the monitor owner.
%%
%% Subsequent events will be either {changed,...} or {error,...}.
%%
%% The monitor reference is not included in the event descriptor itself,
%% but is part of the main message format; see cast/2.

event(#entry{info = Info}, #entry{info = Info}, _Type, _Path) ->
    %% no change in state
    ok;
event(#entry{info = undefined}, #entry{info = NewInfo}=Entry,
      Type, Path)
  when not is_atom(NewInfo) ->
    %% file or directory exists, for a fresh monitor
    Files = [{added, F} || F <- Entry#entry.files],
    cast({exists, Path, Type, NewInfo, Files}, Entry#entry.monitors);
event(_OldEntry, #entry{info = NewInfo}=Entry, Type, Path)
  when is_atom(NewInfo) ->
    %% file is not available
    cast({error, Path, Type, NewInfo}, Entry#entry.monitors);
event(_OldEntry, #entry{info = Info}=Entry, file, Path) ->
    %% a normal file has changed or simply become accessible
    cast({changed, Path, file, Info, []}, Entry#entry.monitors);
event(#entry{info = OldInfo}, #entry{info = NewInfo}=Entry, directory, Path)
  when is_atom(OldInfo) ->
    %% a directory has become accessible
    Files = [{added, F} || F <- Entry#entry.files],
    cast({changed, Path, directory, NewInfo, Files}, Entry#entry.monitors);
event(OldEntry, #entry{info = Info}=Entry, directory, Path) ->
    %% a directory has changed
    Files = diff_lists(Entry#entry.files, OldEntry#entry.files),
    cast({changed, Path, directory, Info, Files}, Entry#entry.monitors).


poll(St) ->
    St#state{files = dict:map(fun (Path, Entry) ->
				      poll_file(Path, Entry, file)
			      end,
			      St#state.files),
	     dirs = dict:map(fun (Path, Entry) ->
				     poll_file(Path, Entry, directory)
			     end,
			     St#state.dirs)}.

poll_file(Path, Entry, Type) ->
    NewEntry = refresh_entry(Path, Entry, Type),
    event(Entry, NewEntry, Type, Path),
    NewEntry.

refresh_entry(Path, Entry, Type) ->
    Info = get_file_info(Path),
    case Type of
	directory when not is_atom(Info) ->
	    case Info#file_info.type of
		directory ->
		    Files = get_dir_files(Path),
		    Entry#entry{info = Info, files = Files};
		_ ->
		    Entry#entry{info = enotdir}
	    end;
	_ ->
	    %% if we're not monitoring this path as a directory, we
	    %% don't care what it is exactly, but just track its status
	    Entry#entry{info = Info}
    end.

%% We clear some fields of the file_info so that we only trigger on real
%% changes; see the //kernel/file.erl manual and file.hrl for details.

get_file_info(Path) ->
    case file:read_file_info(Path) of
	{ok, Info} ->
	    Info#file_info{access = undefined,
			   atime  = undefined};
	{error, Error} ->
	    Error  % posix error code as atom
    end.

%% Listing the members of a directory; note that it yields the empty
%% list if it fails - this is not the place for error detection.

get_dir_files(Path) ->
    case file:list_dir(Path) of
	{ok, Files} -> lists:sort(Files);
	{error, _} -> []
    end.

%% both lists must be sorted for this diff to work

diff_lists([F1 | Fs1], [F2 | _]=Fs2) when F1 < F2 ->
    [{added, F1} | diff_lists(Fs1, Fs2)];
diff_lists([F1 | _]=Fs1, [F2 | Fs2]) when F1 > F2 ->
    [{deleted, F2} | diff_lists(Fs1, Fs2)];
diff_lists([_ | Fs1], [_ | Fs2]) ->
    diff_lists(Fs1, Fs2);
diff_lists([F | Fs1], Fs2) ->
    [{added, F} | diff_lists(Fs1, Fs2)];
diff_lists(Fs1, [F | Fs2]) ->
    [{deleted, F} | diff_lists(Fs1, Fs2)];
diff_lists([], []) ->
    [].


%% Multicasting events to clients. The message has the form
%% {file_monitor, MonitorReference, Event}, where Event is described in
%% more detail above, and 'file_monitor' is the name of this module.

cast(Msg, Monitors) ->
    sets:fold(fun (#monitor{pid = Pid, reference = Ref}, Msg) ->
		      Pid ! {?MSGTAG, Ref, Msg},
		      Msg  % note that this is a fold, not a map
	      end,
	      Msg, Monitors).

%% ------------------------------------------------------------------------
%% Unit tests

-ifdef(EUNIT).

new_test_server() ->
    {ok, Server} = ?MODULE:start(undefined, []),
    Server.

stop_test_server(Server) ->
    ?MODULE:stop(Server).

basic_test_() ->
    %% Start and stop the server for each basic test
    case os:type() of
	{unix,_} ->
	    {foreach,
	     fun new_test_server/0,
	     fun stop_test_server/1,
	     [{with, [fun return_value_test/1]},
	      {with, [fun flatten_path_test/1]},
	      {with, [fun no_file_test/1]},
	      {with, [fun no_dir_test/1]},
	      {with, [fun existing_dir_test/1]},
	      {with, [fun existing_file_test/1]},
	      {with, [fun notdir_test/1]},
	      {with, [fun dir_as_file_test/1]}
	     ]
	    };
	_ ->
	    []
    end.

%% All the below tests run only on unix-like platforms

return_value_test(Server) ->
    Path = "/tmp/nonexisting",  % flat string
    MonitorResult = ?MODULE:monitor_file(Server, Path, self()),
    ?assertMatch({ok, Path, Ref} when is_reference(Ref), MonitorResult),
    {ok, _, MonitorRef} = MonitorResult,
    ?assertMatch(ok, ?MODULE:demonitor(Server, MonitorRef)),
    ?assertMatch({ok, Path, Ref} when is_reference(Ref),
		 ?MODULE:monitor_dir(Server, Path, self())).

flatten_path_test(Server) ->
    Path = ["/","tmp","/","foo"],
    ?assertMatch({ok, "/tmp/foo", _},
		 ?MODULE:monitor_file(Server, Path, self())),
    ?assertMatch({ok, "/tmp/foo", _},
		 ?MODULE:monitor_dir(Server, Path, self())).

no_file_test(Server) ->
    Path = "/tmp/nonexisting",
    {ok, Path, Ref} = ?MODULE:monitor_file(Server, Path, self()),
    receive
	Msg ->
	    ?assertMatch({?MSGTAG, Ref, {error, Path, file, enoent}},
			 Msg)
    end.

no_dir_test(Server) ->
    Path = "/tmp/nonexisting",
    {ok, Path, Ref} = ?MODULE:monitor_dir(Server, Path, self()),
    receive
	Msg ->
	    ?assertMatch({?MSGTAG, Ref, {error, Path, directory, enoent}},
			 Msg)
    end.

existing_dir_test(Server) ->
    Path = "/etc",
    {ok, Path, Ref} = ?MODULE:monitor_dir(Server, Path, self()),
    receive
	Msg ->
	    %% we should get a nonempty list of directory entries
	    ?assertMatch({?MSGTAG, Ref,
			  {exists, Path, directory, #file_info{}, Es}}
			 when (is_list(Es) and (Es =/= [])), Msg)
    end.

existing_file_test(Server) ->
    Path = "/etc/passwd",
    {ok, Path, Ref} = ?MODULE:monitor_file(Server, Path, self()),
    receive
	Msg ->
	    ?assertMatch({?MSGTAG, Ref,
			  {exists, Path, file, #file_info{}, []}}, Msg)
    end.

notdir_test(Server) ->
    Path = "/etc/passwd",
    {ok, Path, Ref} = ?MODULE:monitor_dir(Server, Path, self()),
    receive
	Msg ->
	    ?assertMatch({?MSGTAG, Ref,
			  {error, Path, directory, enotdir}}, Msg)
    end.

dir_as_file_test(Server) ->
    Path = "/etc",
    {ok, Path, Ref} = ?MODULE:monitor_file(Server, Path, self()),
    receive
	Msg ->
	    %% we should get an empty list of directory entries,
	    %% since we are just monitoring it as a file
	    ?assertMatch({?MSGTAG, Ref,
			  {exists, Path, file, #file_info{}, []}}, Msg)
    end.


-endif. %% ifdef EUNIT
