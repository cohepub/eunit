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

-export([monitor_file/1, monitor_file/2, monitor_file/3, monitor_dir/1,
	 monitor_dir/2, monitor_dir/3, automonitor/1, automonitor/2,
	 automonitor/3, demonitor/1, demonitor/2, demonitor_file/2,
	 demonitor_file/3, demonitor_dir/2, demonitor_dir/3,
	 get_poll_time/0, get_poll_time/1, set_poll_time/1,
	 set_poll_time/2, normalize_path/1]).

-export([start/0, start/1, start/2, start_link/0, start_link/1,
	 start_link/2, stop/0, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 code_change/3, terminate/2]).

-include_lib("kernel/include/file.hrl").

%%-include_lib("eunit/include/eunit.hrl").
-include("../include/eunit.hrl").


%% The behaviour of this service is inspired by the open source FAM
%% daemon [http://oss.sgi.com/projects/fam/].
%%
%% NOTE: Monitored paths should be absolute, but this is not checked.
%% 
%% We never rewrite the paths, e.g. from relative to absolute, but we
%% ensure that every path is a flat string internally, for the sake of
%% comparisons, and return it to the caller for reference.

-define(DEFAULT_POLL_TIME, 5000).  % change with option poll_time
-define(MIN_POLL_TIME, 100).
-define(SERVER, ?MODULE).
-define(MSGTAG, ?SERVER).

%% @type object() = {file|directory, filename()}
%% @type monitor() = reference()

-record(state, {poll=true,  % boolean(), false if polling is disabled
		poll_time,  % polling interval (milliseconds)
		files,      % map: file path -> #entry{}
		dirs,       % map: directory path -> #entry{}
		autodirs,   % map: directory path -> monitor() -> entries
		refs,       % map: monitor() -> #monitor_info{}
		clients     % map: client Pid -> #client_info{}
	       }).

-record(entry, {info = undefined,      % #file_info{} or posix atom
		files = [],            % directory entries (if any)
		monitors = sets:new()  % set(monitor())
	       }).

-record(client_info, {monitor,    % erlang:monitor/2 reference
		      refs        % set(monitor()); monitors owned by client
		     }).

-record(monitor_info, {pid,     % client Pid
		       auto,    % boolean(), true for an automonitor
		       objects  % set(object())
		      }).

%%
%% User interface
%%

monitor_file(Path) ->
    monitor_file(Path, []).

monitor_file(Path, Opts) ->
    monitor_file(?SERVER, Path, Opts).

monitor_file(Server, Path, Opts) ->
    monitor(Server, Path, Opts, file).


monitor_dir(Path) ->
    monitor_dir(Path, []).

monitor_dir(Path, Opts) ->
    monitor_dir(?SERVER, Path, Opts).

monitor_dir(Server, Path, Opts) ->
    monitor(Server, Path, Opts, directory).


%% not exported
monitor(Server, Path, Opts, Type) ->
    FlatPath = normalize_path(Path),
    Ref = case proplists:get_value(reference, Opts) of
	      R when is_reference(R) ; R =:= undefined -> R;
	      _ -> erlang:error(badarg)
	  end,
    Cmd = {monitor, self(), {Type, FlatPath}, Ref},
    case gen_server:call(Server, Cmd) of
	{ok, Ref1} -> {ok, Ref1, FlatPath};
	{error, Reason} -> {error, Reason}
    end.


automonitor(Path) ->
    automonitor(Path, []).

automonitor(Path, Opts) ->
    automonitor(?SERVER, Path, Opts).

automonitor(Server, Path, _Opts) ->
    FlatPath = normalize_path(Path),
    {ok, Ref} = gen_server:call(Server, {automonitor, self(), FlatPath}),
    {ok, Ref, FlatPath}.


demonitor(Ref) ->
    demonitor(?SERVER, Ref).

demonitor(Server, Ref) when is_reference(Ref) ->
    ok = gen_server:call(Server, {demonitor, self(), Ref}).

demonitor_file(Path, Ref) ->
    demonitor_file(?SERVER, Path, Ref).

demonitor_file(Server, Path, Ref) ->
    demonitor(Server, Path, Ref, file).

demonitor_dir(Path, Ref) ->
    demonitor_dir(?SERVER, Path, Ref).

demonitor_dir(Server, Path, Ref) ->
    demonitor(Server, Path, Ref, directory).

%% not exported
demonitor(Server, Path, Ref, Type) when is_reference(Ref) ->
    FlatPath = normalize_path(Path),
    ok = gen_server:call(Server, {demonitor, self(), {Type, FlatPath}, Ref}).


get_poll_time() ->
    get_poll_time(?SERVER).

get_poll_time(Server) ->
    gen_server:call(Server, get_poll_time).


set_poll_time(Time) ->
    set_poll_time(?SERVER, Time).

set_poll_time(Server, Time) when is_integer(Time) ->
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
		files = dict:new(),
		dirs = dict:new(),
		autodirs = dict:new(),
		clients = dict:new(),
		refs = dict:new()},
    set_timer(St),
    {ok, St}.

%% Note that we create all references on the server side, to be
%% consistent (and in case it will matter, to ensure that the server
%% mostly uses references local to its own node).

handle_call({monitor, Pid, Object, undefined}, From, St) ->
    handle_call({monitor, Pid, Object, make_ref()}, From, St);
handle_call({monitor, Pid, Object, Ref}, _From, St)
  when is_pid(Pid), is_reference(Ref) ->
    try add_monitor(Object, Pid, Ref, St) of
	St1 -> {reply, {ok, Ref}, register_client(Pid, Ref, St1)}
    catch
	not_owner ->
	    {reply, {error, not_owner}, St};
	automonitor ->
	    {reply, {error, automonitor}, St}
    end;
handle_call({demonitor, Pid, Ref}, _From, St) when is_reference(Ref) ->
    try delete_monitor(Pid, Ref, St) of
	St1 -> {reply, ok, St1}
    catch
	not_owner ->
	    {reply, {error, not_owner}, St}
    end;
handle_call({demonitor, Pid, Object, Ref}, _From, St) when is_reference(Ref) ->
    try demonitor_path(Pid, Ref, Object, St) of
	St1 -> {reply, ok, St1}
    catch
	not_owner ->
	    {reply, {error, not_owner}, St}
    end;
handle_call({automonitor, Pid, Path}, _From, St) when is_pid(Pid) ->
    %% it shouldn't be possible to get exceptions due to wrong owner or
    %% non-automonitor type here, since we always create a new reference
    Ref = make_ref(),
    St1 = unsafe_automonitor_path(Path, Pid, Ref, St),
    {reply, {ok, Ref}, register_client(Pid, Ref, St1)};
handle_call(get_poll_time, _From, St) ->
    {reply, St#state.poll_time, St};
handle_call({set_poll_time, Time}, _From, St) ->
    {reply, ok, St#state{poll_time=safe_poll_time(Time)}};
handle_call(stop, _From, St) ->
    {stop, normal, ok, St}.

handle_cast(_, St) ->
    {noreply, St}.

handle_info({?MSGTAG, Ref, Event}, St) ->
    %% auto-monitoring event to self
    case dict:find(Ref, St#state.refs) of
	{ok, #monitor_info{pid=Pid}} ->
	    {noreply, autoevent(Event, Pid, Ref, St)};
	error ->
	    %% could happen if this event was already in the queue when
	    %% we processed the deletion of the same reference, so just
	    %% ignore the message
	    {noreply, St}
    end;
handle_info(poll_time, St) ->
    {noreply, set_timer(poll(St))};
handle_info(enable_poll, St) ->
    {noreply, St#state{poll=true}};
handle_info({'DOWN', _Ref, process, Pid, _Info}, St) ->
    {noreply, remove_client(Pid, St)};
handle_info(_, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

terminate(_Reason, _St) ->
    ok.

%%
%% Internal functions
%%

%% We allow paths as binaries, atoms, or "extended io-lists" that may
%% contain atoms as well as binaries. This is flattened into a single
%% binary (currently assuming that the input uses an 8-bit encoding).
%% A single character is not a valid path; it must be within a list.

normalize_path(Path) when is_binary(Path) -> Path;
normalize_path(Path) ->
    list_to_binary(flatten_onto(Path, [])).

flatten_onto([X | Xs], As) when is_integer(X), X >= 0, X =< 255 ->
    [X | flatten_onto(Xs, As)];
flatten_onto([X | Xs], As) ->
    flatten_onto(X, flatten_onto(Xs, As));
flatten_onto([], As) ->
    As;
flatten_onto(X, As) when is_atom(X) ->
    atom_to_list(X) ++ As;
flatten_onto(X, As) when is_binary(X) ->
    binary_to_list(X) ++ As;
flatten_onto(_, _) ->
    erlang:error(badarg).

join_to_path(Path, File) when is_binary(Path), is_binary(File) ->
    normalize_path(filename:join(binary_to_list(Path),
				 binary_to_list(File))).

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

%% Handling of auto-monitoring events
%%
%% - If a new entry of an automonitored directory is discovered, make it
%%   too automonitored (this does recursive monitoring by definition).
%%
%% - If an entry is deleted from an automonitored directory, remove any
%%   automonitor from it and its subdirectories recursively. Note that
%%   by definition, this will never auto-remove the top automonitored
%%   directory.
%%
%% - Because of the special status of the top directory (the path given
%%   to automonitor), we do not allow the user to add/remove paths to an
%%   existing automonitor or pass a user-specified reference.
%%
%% - Note that to be able to demonitor directories recursively, we must
%%   track the automonitored directory entries for each directory and
%%   monitor pair (since the directory itself can no longer be read,
%%   there is no other way we can know which subentries to demonitor).
%%
%% - An automonitored non-directory that changes type to directory, or
%%   vice versa, should should cause recreation of the monitor to match
%%   the new type.
%%
%% - Errors on automonitored files are assumed to be intermittent, i.e.,
%%   not even enoent should in itself cause demonitoring - that is done
%%   only if the containing directory reports that the file is removed.
%%
%% - Errors on automonitored directories cause immediate demonitoring of
%%   the entries of the directory, but not the directory itself.

autoevent({_Tag, Path, Type, #file_info{}=Info, _Files}, Pid, Ref, St)
  when (((Type =:= file) and (Info#file_info.type =:= directory)) orelse
	((Type =:= directory) and (Info#file_info.type =/= directory))) ->
    %% monitor type mismatch detected
    autoremonitor_path(Path, Pid, Ref, St);
autoevent({_Tag, Path, directory, #file_info{}=Info, Files}, Pid, Ref, St0)
  when Info#file_info.type =:= directory ->
    %% add/remove automonitoring to/from all added/deleted entries
    lists:foldl(fun ({added, File}, St) ->
			St1 = add_autodir_entry(Path, File, Ref, St),
			automonitor_path(join_to_path(Path, File),
					 Pid, Ref, St1);
		    ({deleted, File}, St) ->
			St1 = remove_autodir_entry(Path, File, Ref, St),
			autodemonitor_path(join_to_path(Path, File),
					   Pid, Ref, St1)
		end,
		St0, Files);
autoevent({error, Path, directory, enotdir}, Pid, Ref, St) ->
    %% monitor type mismatch detected
    autoremonitor_path(Path, Pid, Ref, St);
autoevent({error, Path, directory, _}, Pid, Ref, St) ->
    %% only demonitor subdirectories/files
    autodemonitor_dir_entries(Path, Pid, Ref, St);
autoevent(_Event, _Pid, _Ref, St) ->
    St.

%% monitor type mismatch detected - recreate it to get correct type
autoremonitor_path(Path, Pid, Ref, St) ->
    automonitor_path(Path, Pid, Ref, autodemonitor_path(Path, Pid, Ref, St)).

automonitor_path(Path, Pid, Ref, St) ->
    %% Pid should be a known client, otherwise do nothing
    case dict:is_key(Pid, St#state.clients) of
	true ->
	    try unsafe_automonitor_path(Path, Pid, Ref, St)
	    catch
		throw:_ -> St
	    end;
	false ->
	    St
    end.

%% see add_monitor for possible thrown exceptions
unsafe_automonitor_path(Path, Pid, Ref, St) ->
    Object = case file:read_file_info(binary_to_list(Path)) of
		 {ok, #file_info{type=directory}} ->
		     {directory, Path};
		 _ ->
		     {file, Path}    % also for errors
	     end,
    add_automonitor(Object, Pid, Ref, St).

autodemonitor_path(Path, Pid, Ref, St0) ->
    St1 = try demonitor_path(Pid, Ref, {file, Path}, St0) 
 	  catch
 	      not_owner -> St0
 	  end,
    St2 = try demonitor_path(Pid, Ref, {directory, Path}, St1)
	  catch
	      not_owner -> St1
	  end,
    autodemonitor_dir_entries(Path, Pid, Ref, St2).

autodemonitor_dir_entries(Path, Pid, Ref, St0) ->
    Dirs0 = St0#state.autodirs,
    case dict:find(Path, Dirs0) of
	{ok, Map0} ->
	    case dict:find(Ref, Map0) of
		{ok, Set} ->
		    Map = dict:erase(Ref, Map0),
		    %% purge empty entries to save space
		    Dirs = case dict:size(Map) > 0 of
			       true -> dict:store(Path, Map, Dirs0);
			       false -> dict:erase(Path, Dirs0)
			   end,
		    St1 = St0#state{autodirs = Dirs},
		    sets:fold(fun (File, St) ->
				      P = join_to_path(Path, File),
				      autodemonitor_path(P, Pid, Ref, St)
			      end,
			      St1, Set);
		error ->
		    St0
	    end;
	error ->
	    St0
    end.

%% tracking subentries of automonitored directories, in order to enable
%% efficient recursive demonitoring

add_autodir_entry(Path, File, Ref, St) ->
    Map = case dict:find(Path, St#state.autodirs) of
	      {ok, Map0} ->
		  Set = case dict:find(Ref, Map0) of
			    {ok, Entries} -> Entries;
			    error -> sets:new()
			end,
		  dict:store(Ref, sets:add_element(File, Set), Map0);
	      error ->
		  dict:store(Ref, sets:add_element(File, sets:new()),
			     dict:new())
	  end,
    St#state{autodirs = dict:store(Path, Map, St#state.autodirs)}.

remove_autodir_entry(Path, File, Ref, St) ->
    Dirs0 = St#state.autodirs,
    case dict:find(Path, Dirs0) of
	{ok, Map0} ->
	    case dict:find(Ref, Map0) of
		{ok, Set0} ->
		    %% purge empty entries to save space
		    Set = sets:del_element(File, Set0),
		    Map = case sets:size(Set) > 0 of
			      true -> dict:store(Ref, Set, Map0);
			      false -> dict:erase(Ref, Map0)
			  end,
		    Dirs = case dict:size(Map) > 0 of
			       true -> dict:store(Path, Map, Dirs0);
			       false -> dict:erase(Path, Dirs0)
			   end,
		    St#state{autodirs = Dirs};
		error -> St
	    end;
	error ->
	    St
    end.

%% client monitoring (once a client, always a client - until death)

register_client(Pid, Ref, St) ->
    Info = case dict:find(Pid, St#state.clients) of
	       {ok, OldInfo} -> OldInfo;
	       error ->
		   Monitor = erlang:monitor(process, Pid),
		   #client_info{monitor = Monitor, refs = sets:new()}
	   end,
    Refs = sets:add_element(Ref, Info#client_info.refs),
    St#state{clients = dict:store(Pid, Info#client_info{refs = Refs},
				  St#state.clients)}.

remove_client(Pid, St) ->
    case dict:find(Pid, St#state.clients) of
	{ok, #client_info{monitor = Monitor, refs = Refs}} ->
	    erlang:demonitor(Monitor, [flush]),
	    purge_client(Pid, Refs, St);
	error ->
	    St
    end.

purge_client(Pid, Refs, St0) ->
    sets:fold(fun (Ref, St) ->
		      %% the Pid *should* be the owner here, so
		      %% a not_owner exception should not happen
		      delete_monitor(Pid, Ref, St)
	      end,
	      St0#state{clients = dict:erase(Pid, St0#state.clients)},
	      Refs).

%% Adding a new monitor; throws 'not_owner' if the monitor reference is
%% already registered for another Pid; throws 'automonitor' if the
%% reference is already registered with a different type.

add_monitor(Object, Pid, Ref, St) ->
    add_monitor(Object, Pid, Ref, St, false).

add_automonitor(Object, Pid, Ref, St) ->
    add_monitor(Object, Pid, Ref, St, true).

add_monitor(Object, Pid, Ref, St, Auto) ->
    Info = case dict:find(Ref, St#state.refs) of
	       {ok, #monitor_info{pid = Pid, auto = Auto}=OldInfo} ->
		   OldInfo;
	       {ok, #monitor_info{pid = Pid}} ->
		   throw(automonitor);
	       {ok, #monitor_info{}} ->
		   throw(not_owner);
	       error ->
		   #monitor_info{pid = Pid, auto = Auto,
				 objects = sets:new()}
	   end,
    NewObjects = sets:add_element(Object, Info#monitor_info.objects),
    Refs = dict:store(Ref, Info#monitor_info{objects = NewObjects},
		      St#state.refs),
    monitor_path(Object, Ref, St#state{refs = Refs}).

%% We must separate the namespaces for files and dirs; there may be
%% simultaneous file and directory monitors for the same path, and a
%% file may be deleted and replaced by a directory of the same name, or
%% vice versa. The client should know (more or less) if a path is
%% expected to refer to a file or a directory.

monitor_path({file, Path}, Ref, St) ->
    St#state{files = monitor_path(Path, Ref, file, St#state.files, St)};
monitor_path({directory, Path}, Ref, St) ->
    St#state{dirs = monitor_path(Path, Ref, directory, St#state.dirs, St)}.

%% Adding a new monitor forces an immediate poll of the path, such that
%% previous monitors only see any real change, while the new monitor
%% either gets {found, ...} or {error, ...}.

monitor_path(Path, Ref, Type, Dict, St) ->
    Entry = case dict:find(Path, Dict) of
		{ok, OldEntry} -> poll_file(Path, OldEntry, Type, St);
		error -> new_entry(Path, Type)
	    end,
    event(#entry{}, dummy_entry(Entry, Ref), Type, Path, St),
    NewEntry = Entry#entry{monitors =
			   sets:add_element(Ref, Entry#entry.monitors)},
    dict:store(Path, NewEntry, Dict).

dummy_entry(Entry, Ref) ->
    Entry#entry{monitors = sets:add_element(Ref, sets:new())}.

new_entry(Path, Type) ->
    refresh_entry(Path, #entry{monitors = sets:new()}, Type).

%% Deleting a monitor by reference; throws not_owner if the monitor
%% reference is owned by another Pid. The client_info entry may already
%% have been deleted if we come from purge_client().

delete_monitor(Pid, Ref, St0) ->
    St1 = case dict:find(Pid, St0#state.clients) of
	      {ok, #client_info{refs = Refs}=I} ->
		  NewRefs = sets:del_element(Ref, Refs),
		  St0#state{clients =
			    dict:store(Pid, I#client_info{refs = NewRefs},
				       St0#state.clients)};
	      error ->
		  St0
	  end,
    case dict:find(Ref, St1#state.refs) of
	{ok, #monitor_info{pid = Pid, objects = Objects}} ->
	    sets:fold(fun (Object, St) ->
			      purge_monitor_path(Ref, Object, St)
		      end,
		      St1#state{refs = dict:erase(Ref, St1#state.refs)},
		      Objects);
	{ok, #monitor_info{}} -> throw(not_owner);
	error ->
	    St1
    end.

%% Deleting a particular path from a monitor. Throws not_owner if the
%% monitor reference is owned by another Pid.

demonitor_path(Pid, Ref, Object, St) ->
    case dict:find(Ref, St#state.refs) of
	{ok, #monitor_info{pid = Pid, objects = Objects}=I} ->
	    St1 = purge_monitor_path(Ref, Object, St),
	    I1 = I#monitor_info{objects=sets:del_element(Object, Objects)},
	    St1#state{refs = dict:store(Ref, I1, St1#state.refs)};
	{ok, #monitor_info{}} -> throw(not_owner);
	error ->
	    St
    end.

%% Deleting a particular monitor from a path.

purge_monitor_path(Ref, {file, Path}, St) ->
    St#state{files = purge_monitor_path_1(Path, Ref, St#state.files)};
purge_monitor_path(Ref, {directory, Path}, St) ->
    St#state{dirs = purge_monitor_path_1(Path, Ref, St#state.dirs)}.

purge_monitor_path_1(Path, Ref, Dict) ->
    case dict:find(Path, Dict) of
	{ok, Entry} -> 
	    Monitors = sets:del_element(Ref, Entry#entry.monitors),
	    case sets:size(Monitors) > 0 of
		true ->
		    dict:store(Path, Entry#entry{monitors = Monitors}, Dict);
		false ->
		    dict:erase(Path, Dict)
	    end;
	error ->
	    Dict
    end.

%% Generating events upon state changes by comparing old and new states
%% 
%% Event formats:
%%   {found, Path, Type, #file_info{}, Files}
%%   {changed, Path, Type, #file_info{}, Files}
%%   {error, Path, Type, PosixAtom}
%%
%% Type is file or directory, as specified by the monitor type, not by
%% the actual type on disk. If Type is file, Files is always []. If Type
%% is directory, Files is a list of {added, FileName} and {deleted,
%% FileName}, where FileName is on basename form, i.e., without any
%% directory component.
%%
%% When a new monitor is installed for a path, an initial {found,...}
%% or {error,...} event will be sent to the monitor owner.
%%
%% Subsequent events will be either {changed,...} or {error,...}.
%%
%% The monitor reference is not included in the event descriptor itself,
%% but is part of the main message format; see cast/2.

event(#entry{info = Info}, #entry{info = Info}, _Type, _Path, _St) ->
    %% no change in state (note that we never compare directory entry
    %% lists here; if there have been changes, the timestamps should
    %% also be different - see list_dir/2 below)
    ok;
event(#entry{info = undefined}, #entry{info = NewInfo}=Entry,
      Type, Path, St)
  when not is_atom(NewInfo) ->
    %% file or directory exists, for a fresh monitor
    Files = diff_lists(Entry#entry.files, []),
    cast({found, Path, Type, NewInfo, Files}, Entry#entry.monitors, St);
event(OldEntry, #entry{info = NewInfo}=Entry, Type, Path, St)
  when is_atom(NewInfo) ->
    %% file or directory is not available
    type_change_event(OldEntry#entry.info, NewInfo, Type, Path, Entry, St),
    cast({error, Path, Type, NewInfo}, Entry#entry.monitors, St);
event(OldEntry, #entry{info = NewInfo}=Entry, Type, Path, St) ->
    %% a file or directory has changed or become readable again after an error
    type_change_event(OldEntry#entry.info, NewInfo, Type, Path, Entry, St),
    Files = diff_lists(Entry#entry.files, OldEntry#entry.files),
    cast({changed, Path, Type, NewInfo, Files}, Entry#entry.monitors, St).

%% sudden changes in file type cause an 'enoent' error report before the
%% new status, so that clients do not need to detect this themselves
type_change_event(#file_info{type = T}, #file_info{type = T}, _, _, _, _) ->
    ok;
type_change_event(#file_info{}, #file_info{}, Type, Path, Entry, St) ->
    cast_enoent(Type, Path, Entry, St);
type_change_event(#file_info{type = directory}, enotdir, Type, Path, Entry,
		  St) ->
    cast_enoent(Type, Path, Entry, St);
type_change_event(enotdir, #file_info{type = directory}, Type, Path, Entry,
		  St) ->
    cast_enoent(Type, Path, Entry, St);
type_change_event(_, _, _, _, _, _) ->
    ok.

cast_enoent(Type, Path, Entry, St) ->
    cast({error, Path, Type, enoent}, Entry#entry.monitors, St).

poll(#state{poll=false}=St) ->
    St;
poll(#state{poll=true}=St) ->
    Files = dict:map(fun (Path, Entry) ->
			     poll_file(Path, Entry, file, St)
		     end,
		     St#state.files),
    Dirs = dict:map(fun (Path, Entry) ->
			    poll_file(Path, Entry, directory, St)
		    end,
		    St#state.dirs),
    %% polling will now be disabled until all automonitoring events have
    %% been processed:
    self() ! enable_poll,
    St#state{poll=false, files = Files, dirs = Dirs}.

poll_file(Path, Entry, Type, St) ->
    NewEntry = refresh_entry(Path, Entry, Type),
    event(Entry, NewEntry, Type, Path, St),
    NewEntry.

refresh_entry(Path0, Entry, Type) ->
    Path = binary_to_list(Path0),
    OldInfo = Entry#entry.info,
    Info = get_file_info(Path),
    case Type of
	directory when not is_atom(Info) ->
	    case Info#file_info.type of
		directory when is_atom(OldInfo)
		; Info#file_info.mtime =/= OldInfo#file_info.mtime ->
		    %% we only list directories when they have new
		    %% timestamps, to keep the polling cost down
		    Files = list_dir(Path, Info#file_info.mtime),
		    Entry#entry{info = Info, files = Files};
		directory ->
		    Entry#entry{info = Info};
		_ ->
		    %% attempting to monitor a non-directory as a
		    %% directory is reported as an 'enotdir' error
		    Entry#entry{info = enotdir, files = []}
	    end;
	_ ->
	    %% If we're not monitoring this path as a directory, we
	    %% don't care what it is exactly, but just track its status.
	    %% To handle the case of an error on a directory monitor, we
	    %% make sure to reset the list of entries.
	    Entry#entry{info = Info, files = []}
    end.

%% We clear some fields of the file_info so that we only trigger on real
%% changes; see the //kernel/file.erl manual and file.hrl for details.

get_file_info(Path) when is_list(Path) ->
    case file:read_file_info(Path) of
	{ok, Info} ->
	    Info#file_info{access = undefined,
			   atime  = undefined};
	{error, Error} ->
	    Error  % posix error code as atom
    end.

%% Listing the members of a directory; note that it yields the empty
%% list if it fails - this is not the place for error detection.

list_dir(Path, Mtime) when is_list(Path) ->
    Files = case file:list_dir(Path) of
		{ok, Fs} -> [normalize_path(F) || F <- Fs];
		{error, _} -> []
	    end,
    %% The directory access time should now be the time when we read the
    %% listing (at least), i.e., the current time.
    case file:read_file_info(Path) of
	{ok, #file_info{atime = Atime}} when Mtime =:= Atime ->
	    %% Recall that the timestamps have whole-second resolution.
	    %% If we listed the directory during the same second that it
	    %% was (or is still being) modified, it is possible that we
	    %% did not see all the additions/deletions that were made
	    %% that second - and in that case, we will not report those
	    %% changes until the timestamp changes again (maybe never).
	    %% To avoid that, we pause and list the directory again.
	    %% This of course affects the latency of this server in
	    %% certain situations, so a better solution would be to
	    %% abort the current refresh for this directory and schedule
	    %% it for a new asynchronous refresh after a small delay
	    %% (unless the poll interval is already below 0.5 seconds).
	    receive after 550 -> list_dir(Path, Mtime) end;
	_ ->
	    %% ignore possible errors when reading file_info as well
	    lists:sort(Files)
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

cast(Msg, Monitors, St) ->
    sets:fold(fun (Ref, Msg) ->
		      case dict:find(Ref, St#state.refs) of
			  {ok, #monitor_info{pid = Pid, auto = Auto}} ->
			      Pid ! {?MSGTAG, Ref, Msg},
			      case Auto of
				  true -> self() ! {?MSGTAG, Ref, Msg};
				  false -> ok
			      end
		      end,
		      Msg  % note that this is a fold, not a map
	      end,
	      Msg, Monitors).
