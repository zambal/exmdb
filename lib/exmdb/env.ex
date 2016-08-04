defmodule Exmdb.Env do
  defstruct res: nil, path: nil, dbs: nil, opts: []

  @type db_name :: String.t

  @type db_spec :: [{:key_type, :binary | :term | :ordered_term} |
                    {:val_type, :binary | :term | :ordered_term} |
                    {:reverse_key, boolean} |
                    {:dup_sort, boolean} |
                    {:reverse_dup, boolean}]

  @type db_specs :: %{required(db_name) => db_spec} | db_spec

  @type env_opt :: {:map_size, non_neg_integer} |
                   {:read_only, boolean} |
                   {:write_map, boolean} |
                   {:meta_sync, boolean} |
                   {:sync, boolean | :async} |
                   {:read_ahead, boolean}

  @type env_opts :: [env_opt]

  @type env_create_opt :: {:dbs, db_specs} | {:force, boolean}

  @type env_create_opts :: [env_opt | env_create_opt]

  @type db :: {binary, db_spec}

  @type dbs :: %{required(db_name) => db} | db

  @opaque t :: %Exmdb.Env{res: binary, path: Path.t, dbs: dbs, opts: env_opts}


  def create(path, opts \\ []) do
    if exists?(path, opts) do
      {:error, :exits}
    else
      {:ok, path
       |> open_env(opts)
       |> open_dbs(opts, true)
       |> write_config(opts)}
    end
  end

  def open(path, opts \\ []) do
    if exists?(path, opts) do
      db_specs = read_config(path)
      opts = Keyword.put(opts, :dbs, db_specs)

      {:ok, path
       |> open_env(opts)
       |> open_dbs(opts, false)}
    else
      {:error, :not_found}
    end
  end

  def close(%Exmdb.Env{res: res}) do
    case :elmdb.env_close(res) do
      :ok ->
        :ok
      {:error, {_code, msg}} ->
        raise List.to_string(msg)
    end
  end

  defp exists?(path, opts) do
    if Keyword.get(opts, :force) do
      false
    else
      path
      |> config_path()
      |> File.exists?()
    end
  end

  defp open_env(path, opts) do
    env_opts = build_env_opts(opts)
    result = path
    |> :unicode.characters_to_list()
    |> :elmdb.env_open(env_opts)

    case result do
      {:ok, res} ->
        # drop create opts
        opts = Keyword.drop(opts, [:dbs, :force])
        %Exmdb.Env{res: res, path: path, opts: opts}
      {:error, {_code, msg}} ->
        raise List.to_string(msg)
    end
  end

  defp build_env_opts(opts) do
    env_opts = Keyword.take(opts, [:map_size])

    env_opts = if Keyword.get(opts, :read_only) do
      [:read_only | env_opts]
    else
      env_opts
    end

    env_opts = if Keyword.get(opts, :write_map) do
      [:write_map | env_opts]
    else
      env_opts
    end

    env_opts = if Keyword.get(opts, :meta_sync, true) do
      env_opts
    else
      [:no_meta_sync | env_opts]
    end

    env_opts = if sync = Keyword.get(opts, :sync, true) do
      if sync == :async do
        [:map_async | env_opts]
      else
        env_opts
      end
    else
      [:no_sync | env_opts]
    end

    env_opts = if Keyword.get(opts, :read_ahead, true) do
      env_opts
    else
      [:no_read_ahead | env_opts]
    end

    if dbs = Keyword.get(opts, :dbs) do
      [{:max_dbs, Enum.count(dbs)} | env_opts]
    else
      env_opts
    end
  end

  defp open_dbs(%Exmdb.Env{res: res} = env, opts, create) do
    db_specs = Keyword.get(opts, :dbs, [])
    dbs = if is_map(db_specs) do
      for {name, spec} <- db_specs, into: %{} do
        {name, {open_db(res, name, spec, create), spec}}
      end
    else
      {open_db(res, "", db_specs, create), db_specs}
    end
    %Exmdb.Env{env|dbs: dbs}
  end

  defp open_db(env_res, name, spec, create) do
    db_opts = build_db_opts(spec, create)
    case :elmdb.db_open(env_res, name, db_opts) do
      {:ok, dbi_res} ->
        dbi_res
      {:error, {_code, msg}} ->
        raise List.to_string(msg)
    end
  end

  defp build_db_opts(opts, create) do
    db_opts = if create, do: [:create], else: []

    db_opts = if Keyword.get(opts, :reverse_key) do
      [:reverse_key | db_opts]
    else
      db_opts
    end

    db_opts = if Keyword.get(opts, :dup_sort) do
      [:dup_sort | db_opts]
    else
      db_opts
    end

    if Keyword.get(opts, :reverse_dup) do
      [:reverse_dup | db_opts]
    else
      db_opts
    end
  end

  defp write_config(%Exmdb.Env{path: path} = env, opts) do
    bin_db_specs = opts
    |> Keyword.get(:dbs, [])
    |> :erlang.term_to_binary()

    :ok = path
    |> config_path()
    |> File.write!(bin_db_specs)

    env
  end

  defp read_config(path) do
    path
    |> config_path()
    |> File.read!()
    |> :erlang.binary_to_term()
  end

  defp config_path(path) do
    Path.join(path, "config.exmdb")
  end
end

defimpl Inspect, for: Exmdb.Env do
  import Inspect.Algebra

  def inspect(%Exmdb.Env{path: path}, _opts) do
    concat [ "#Exmdb.Env<", path, ">" ]
  end
end
