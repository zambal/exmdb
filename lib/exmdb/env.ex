defmodule Exmdb.Env do
  import Exmdb.Util

  defstruct res: nil, path: nil, dbs: nil, opts: []

  @type db_name :: String.t

  @type data_type :: :binary | :term | :ordered_term

  @type db_config :: [{:key_type, data_type} |
                      {:val_type, data_type} |
                      {:reverse_key, boolean} |
                      {:dup_sort, boolean} |
                      {:reverse_dup, boolean}]

  @type dbs_config :: %{required(db_name) => db_config} | db_config

  @type env_opt :: {:map_size, non_neg_integer} |
                   {:read_only, boolean} |
                   {:write_map, boolean} |
                   {:meta_sync, boolean} |
                   {:sync, boolean | :async} |
                   {:read_ahead, boolean}

  @type env_opts :: [env_opt]

  @type env_create_opt :: {:dbs, dbs_config} | {:force, boolean}

  @type env_create_opts :: [env_opt | env_create_opt]

  @type db_spec :: {binary, data_type, data_type}

  @type dbs :: %{required(db_name) => db_spec} | db_spec

  @type t :: %Exmdb.Env{res: binary, path: Path.t, dbs: dbs, opts: env_opts}


  def create(path, opts \\ []) do
    if exists?(path, opts) do
      {:error, :exists}
    else
      {:ok, path
       |> open_env(opts)
       |> open_dbs(opts, true)
       |> write_config(opts)}
    end
  end

  def open(path, opts \\ []) do
    if exists?(path, opts) do
      dbs_config = read_config(path)
      opts = Keyword.put(opts, :dbs, dbs_config)

      {:ok, path
       |> open_env(opts)
       |> open_dbs(opts, false)}
    else
      {:error, :not_found}
    end
  end

  def close(%Exmdb.Env{res: res}) do
    :elmdb.env_close(res)
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
      {:error, e} ->
        mdb_error(e)
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

    env_opts = if Keyword.get(opts, :mem_init, true) do
      env_opts
    else
      [:no_mem_init | env_opts]
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
    dbs_config = Keyword.get(opts, :dbs, [])
    dbs = if is_map(dbs_config) do
      for {name, config} <- dbs_config, into: %{} do
        dbi = open_db(res, name, config, create)
        key_type = Keyword.get(config, :key_type, :binary)
        val_type = Keyword.get(config, :val_type, :term)
        {name, {dbi, key_type, val_type}}
      end
    else
      dbi = open_db(res, "", dbs_config, create)
      key_type = Keyword.get(dbs_config, :key_type, :binary)
      val_type = Keyword.get(dbs_config, :val_type, :term)
      {dbi, key_type, val_type}
    end
    %Exmdb.Env{env|dbs: dbs}
  end

  defp open_db(env_res, name, config, create) do
    opts = build_db_opts(config, create)
    case :elmdb.db_open(env_res, name, opts) do
      {:ok, dbi_res} -> dbi_res
      {:error, e}    -> mdb_error(e)
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
    bin_config = opts
    |> Keyword.get(:dbs, [])
    |> :erlang.term_to_binary()

    :ok = path
    |> config_path()
    |> File.write!(bin_config)

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
